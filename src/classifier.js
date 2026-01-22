import { getTemperatureStats, getHourlyTemperatures, getLocationMobility, getTemperatureStatsAtLocation, getCurrentLocation } from './clickhouse-client.js';
import { getHistoricalTemperature, compareSensorToWeather, calculateWeatherComparisonStats } from './weather-fetcher.js';

/**
 * Classification thresholds (tunable)
 */
const THRESHOLDS = {
    // Temperature variance - indoor sensors have low variance
    INDOOR_MAX_VARIANCE: 4,      // Indoor typically < 4°C variance
    OUTDOOR_MIN_VARIANCE: 8,     // Outdoor typically > 8°C variance

    // Temperature range
    INDOOR_MAX_RANGE: 10,        // Indoor rarely swings > 10°C
    OUTDOOR_MIN_RANGE: 15,       // Outdoor often swings > 15°C

    // Absolute temperature bounds
    INDOOR_MIN_TEMP: 10,         // Indoor rarely below 10°C
    INDOOR_MAX_TEMP: 35,         // Indoor rarely above 35°C

    // Weather correlation
    OUTDOOR_MIN_CORRELATION: 0.7,  // Outdoor should correlate well with weather
    INDOOR_MAX_CORRELATION: 0.4,   // Indoor typically doesn't correlate with weather

    // Confidence thresholds
    HIGH_CONFIDENCE: 0.8,
    MEDIUM_CONFIDENCE: 0.5,
    LOW_CONFIDENCE: 0.3,

    // Device temperature detection (suspicious readings - above ambient)
    DEVICE_TEMP_MIN_AVG_DIFF: 10,     // If avg is >10°C above weather, likely device temp
    DEVICE_TEMP_MIN_DIFF: 8,          // If min temp is >8°C above weather min, suspicious
    DEVICE_TEMP_HIGH_BASELINE: 20,    // If sensor min temp is >20°C when weather was cold, suspicious

    // Anomalous temperature detection (suspicious readings - below ambient or other issues)
    ANOMALY_MIN_AVG_DIFF_BELOW: 5,    // If avg is >5°C BELOW weather, suspicious (measuring something cold)
    ANOMALY_MAX_DIFF_BELOW: 8,        // If max temp is >8°C below weather max, very suspicious

    // Mobility detection (location_type)
    FIXED_MAX_RANGE_KM: 0.5,          // Fixed sensors move < 0.5km (GPS drift)
    MOBILE_MIN_RANGE_KM: 2.0,         // Mobile sensors move > 2km
    PORTABLE_MIN_RANGE_KM: 50.0,      // Portable/traveling > 50km (vehicle, boat, etc.)
    MIN_LOCATIONS_FOR_MOBILE: 3       // Need at least 3 unique locations to call mobile
};

/**
 * Individual detection strategies
 */
const strategies = {
    /**
     * Variance analysis - indoor sensors have stable temperatures
     */
    async varianceAnalysis(sensor, days = 7) {
        const stats = await getTemperatureStats(sensor.device_id, days);
        if (!stats || stats.reading_count < 24) {
            return { score: 0, confidence: 0, reason: 'Insufficient data' };
        }

        const variance = parseFloat(stats.temp_variance);
        const stddev = parseFloat(stats.temp_stddev);
        const range = parseFloat(stats.temp_range);

        // Low variance suggests indoor
        let indoorScore = 0;
        if (variance < THRESHOLDS.INDOOR_MAX_VARIANCE) {
            indoorScore += 0.4;
        }
        if (range < THRESHOLDS.INDOOR_MAX_RANGE) {
            indoorScore += 0.3;
        }
        if (stddev < 2) {
            indoorScore += 0.3;
        }

        // High variance suggests outdoor
        let outdoorScore = 0;
        if (variance > THRESHOLDS.OUTDOOR_MIN_VARIANCE) {
            outdoorScore += 0.4;
        }
        if (range > THRESHOLDS.OUTDOOR_MIN_RANGE) {
            outdoorScore += 0.3;
        }
        if (stddev > 4) {
            outdoorScore += 0.3;
        }

        const classification = indoorScore > outdoorScore ? 'INDOOR' : 'OUTDOOR';
        const confidence = Math.abs(indoorScore - outdoorScore);

        return {
            classification,
            confidence: Math.min(confidence, 1),
            reason: `Variance: ${variance.toFixed(2)}, Range: ${range.toFixed(1)}°C, StdDev: ${stddev.toFixed(2)}`,
            stats: { variance, range, stddev }
        };
    },

    /**
     * Weather correlation - outdoor sensors should correlate with local weather
     * @param {Object} sensor - Sensor object with device_id
     * @param {number} days - Number of days of data
     * @param {Array} weatherData - Pre-fetched weather data (optional, fetched if not provided)
     * @param {Array} sensorData - Pre-fetched sensor hourly data (optional, fetched if not provided)
     */
    async weatherCorrelation(sensor, days = 7, weatherData = null, sensorData = null) {
        const lat = parseFloat(sensor.latitude);
        const lng = parseFloat(sensor.longitude);

        if (!lat || !lng || lat === 0 || lng === 0) {
            return { score: 0, confidence: 0, reason: 'No location data' };
        }

        // Get sensor hourly data if not provided
        if (!sensorData) {
            sensorData = await getHourlyTemperatures(sensor.device_id, days);
        }
        if (sensorData.length < 24) {
            return { score: 0, confidence: 0, reason: 'Insufficient sensor data' };
        }

        // Get weather data for same period if not provided
        if (!weatherData) {
            weatherData = await getHistoricalTemperature(lat, lng, days);
        }
        if (weatherData.length < 24) {
            return { score: 0, confidence: 0, reason: 'Insufficient weather data' };
        }

        // Calculate correlation
        const { correlation, matchedHours } = compareSensorToWeather(sensorData, weatherData);

        if (matchedHours < 24) {
            return { score: 0, confidence: 0, reason: `Only ${matchedHours} matched hours` };
        }

        let classification;
        let confidence;

        if (correlation >= THRESHOLDS.OUTDOOR_MIN_CORRELATION) {
            classification = 'OUTDOOR';
            confidence = (correlation - 0.5) * 2; // Scale 0.5-1.0 to 0-1
        } else if (correlation <= THRESHOLDS.INDOOR_MAX_CORRELATION) {
            classification = 'INDOOR';
            confidence = (0.5 - correlation) * 2; // Scale 0-0.5 to 0-1
        } else {
            // Ambiguous correlation (0.4-0.7) - don't classify, leave as unknown
            return {
                classification: null,
                confidence: 0,
                reason: `Ambiguous correlation: ${correlation.toFixed(3)} (${matchedHours} hours)`,
                stats: { correlation, matchedHours }
            };
        }

        return {
            classification,
            confidence: Math.min(Math.max(confidence, 0), 1),
            reason: `Weather correlation: ${correlation.toFixed(3)} (${matchedHours} hours)`,
            stats: { correlation, matchedHours }
        };
    },

    /**
     * Temperature range heuristics - indoor stays within comfortable bounds
     */
    async temperatureRange(sensor, days = 7) {
        const stats = await getTemperatureStats(sensor.device_id, days);
        if (!stats || stats.reading_count < 24) {
            return { score: 0, confidence: 0, reason: 'Insufficient data' };
        }

        const minTemp = parseFloat(stats.min_temp);
        const maxTemp = parseFloat(stats.max_temp);
        const avgTemp = parseFloat(stats.avg_temp);

        // Check if temperature stays within indoor bounds
        const staysWarm = minTemp >= THRESHOLDS.INDOOR_MIN_TEMP;
        const staysCool = maxTemp <= THRESHOLDS.INDOOR_MAX_TEMP;
        const comfortableAvg = avgTemp >= 15 && avgTemp <= 28;

        let indoorScore = 0;
        if (staysWarm) indoorScore += 0.35;
        if (staysCool) indoorScore += 0.35;
        if (comfortableAvg) indoorScore += 0.3;

        // Check for outdoor indicators
        let outdoorScore = 0;
        if (minTemp < 5) outdoorScore += 0.4;  // Gets cold
        if (maxTemp > 38) outdoorScore += 0.3; // Gets hot
        if (maxTemp - minTemp > 20) outdoorScore += 0.3; // Wide swing

        const classification = indoorScore > outdoorScore ? 'INDOOR' : 'OUTDOOR';
        const confidence = Math.abs(indoorScore - outdoorScore);

        return {
            classification,
            confidence: Math.min(confidence, 1),
            reason: `Range: ${minTemp.toFixed(1)}°C to ${maxTemp.toFixed(1)}°C, Avg: ${avgTemp.toFixed(1)}°C`,
            stats: { minTemp, maxTemp, avgTemp }
        };
    },

    /**
     * Device temperature detection - identifies sensors reporting chip/device temp
     * instead of ambient air temperature (consistently above weather)
     * @param {Object} sensor - Sensor object with device_id
     * @param {number} days - Number of days of data
     * @param {Array} weatherData - Pre-fetched weather data (optional, fetched if not provided)
     * @param {Array} sensorData - Pre-fetched sensor hourly data (optional, fetched if not provided)
     */
    async deviceTempDetection(sensor, days = 7, weatherData = null, sensorData = null) {
        const lat = parseFloat(sensor.latitude);
        const lng = parseFloat(sensor.longitude);

        if (!lat || !lng || lat === 0 || lng === 0) {
            return { suspicious: false, confidence: 0, reason: 'No location data' };
        }

        // Get sensor hourly data if not provided
        if (!sensorData) {
            sensorData = await getHourlyTemperatures(sensor.device_id, days);
        }
        if (sensorData.length < 24) {
            return { suspicious: false, confidence: 0, reason: 'Insufficient sensor data' };
        }

        // Get weather data if not provided
        if (!weatherData) {
            weatherData = await getHistoricalTemperature(lat, lng, days);
        }
        if (weatherData.length < 24) {
            return { suspicious: false, confidence: 0, reason: 'Insufficient weather data' };
        }

        // Calculate comparison stats
        const stats = calculateWeatherComparisonStats(sensorData, weatherData);
        if (!stats) {
            return { suspicious: false, confidence: 0, reason: 'Could not calculate stats' };
        }

        // Detection logic
        let suspiciousScore = 0;
        const reasons = [];
        let anomalyType = null;

        // Check if consistently ABOVE ambient (device/chip heat)
        if (stats.diff.avg > THRESHOLDS.DEVICE_TEMP_MIN_AVG_DIFF) {
            suspiciousScore += 0.4;
            reasons.push(`Avg +${stats.diff.avg.toFixed(1)}°C above weather`);
            anomalyType = 'ABOVE_AMBIENT';
        }

        // Check if minimum temp is way above weather minimum
        if (stats.diff.min > THRESHOLDS.DEVICE_TEMP_MIN_DIFF) {
            suspiciousScore += 0.3;
            reasons.push(`Min +${stats.diff.min.toFixed(1)}°C above weather min`);
            anomalyType = anomalyType || 'ABOVE_AMBIENT';
        }

        // Check for high baseline when weather was cold
        if (stats.sensor.min > THRESHOLDS.DEVICE_TEMP_HIGH_BASELINE && stats.weather.min < 10) {
            suspiciousScore += 0.3;
            reasons.push(`Never below ${stats.sensor.min.toFixed(1)}°C when weather hit ${stats.weather.min.toFixed(1)}°C`);
            anomalyType = anomalyType || 'ABOVE_AMBIENT';
        }

        // Check if consistently BELOW ambient (measuring something cold - pipe, water, underground, shade)
        if (stats.diff.avg < -THRESHOLDS.ANOMALY_MIN_AVG_DIFF_BELOW) {
            suspiciousScore += 0.5;
            reasons.push(`Avg ${stats.diff.avg.toFixed(1)}°C below weather (measuring something cold?)`);
            anomalyType = 'BELOW_AMBIENT';
        }

        // Check if max temp is way below weather max (never warms up with weather)
        const maxDiff = stats.sensor.max - stats.weather.max;
        if (maxDiff < -THRESHOLDS.ANOMALY_MAX_DIFF_BELOW) {
            suspiciousScore += 0.3;
            reasons.push(`Max ${maxDiff.toFixed(1)}°C below weather max (shaded/underground?)`);
            anomalyType = anomalyType || 'BELOW_AMBIENT';
        }

        const isSuspicious = suspiciousScore >= 0.5;

        return {
            suspicious: isSuspicious,
            anomalyType,
            confidence: Math.min(suspiciousScore, 1),
            reason: reasons.length > 0 ? reasons.join('; ') : 'Temperature matches weather',
            stats: {
                sensorAvg: stats.sensor.avg,
                weatherAvg: stats.weather.avg,
                avgDiff: stats.diff.avg,
                sensorMin: stats.sensor.min,
                sensorMax: stats.sensor.max,
                weatherMin: stats.weather.min,
                weatherMax: stats.weather.max,
                correlation: stats.correlation
            }
        };
    },

    /**
     * Mobility analysis - determines if sensor is FIXED, MOBILE, or PORTABLE
     * based on location changes over time
     */
    async mobilityAnalysis(sensor, days = 7) {
        const mobility = await getLocationMobility(sensor.device_id, days);

        if (!mobility || mobility.reading_count < 10) {
            return {
                location_type: 'UNKNOWN',
                confidence: 0,
                reason: 'Insufficient location data',
                stats: null
            };
        }

        const { range_km, unique_locations } = mobility;

        // Determine location type
        let locationType;
        let confidence;
        let reason;

        if (range_km < THRESHOLDS.FIXED_MAX_RANGE_KM) {
            locationType = 'FIXED';
            confidence = 0.9;
            reason = `Stationary (${range_km.toFixed(2)}km range, ${unique_locations} locations)`;
        } else if (range_km >= THRESHOLDS.PORTABLE_MIN_RANGE_KM) {
            locationType = 'PORTABLE';
            confidence = 0.95;
            reason = `Long distance travel (${range_km.toFixed(1)}km range, ${unique_locations} locations)`;
        } else if (range_km >= THRESHOLDS.MOBILE_MIN_RANGE_KM && unique_locations >= THRESHOLDS.MIN_LOCATIONS_FOR_MOBILE) {
            locationType = 'MOBILE';
            confidence = 0.8;
            reason = `Mobile (${range_km.toFixed(1)}km range, ${unique_locations} locations)`;
        } else if (range_km >= THRESHOLDS.MOBILE_MIN_RANGE_KM) {
            // Moved but not many unique locations - could be a one-time relocation
            locationType = 'RELOCATED';
            confidence = 0.6;
            reason = `Possibly relocated (${range_km.toFixed(1)}km range, ${unique_locations} locations)`;
        } else {
            // In between - slight movement, likely GPS drift or minor repositioning
            locationType = 'FIXED';
            confidence = 0.5;
            reason = `Minor movement (${range_km.toFixed(2)}km range, likely GPS drift)`;
        }

        return {
            location_type: locationType,
            confidence,
            reason,
            stats: {
                range_km,
                unique_locations,
                reading_count: mobility.reading_count
            }
        };
    }
};

/**
 * Classify a single sensor using all strategies
 *
 * Two-pass approach:
 * - Variance and temperature range use extended history (90 days) to capture more data
 * - Weather correlation uses recent data (specified days) since weather API is limited
 * - For RELOCATED sensors, variance analysis uses only data from current location
 */
export async function classifySensor(sensor, days = 7) {
    const results = {};
    const extendedDays = 90; // Use 90 days of history for non-weather strategies

    // Run mobility analysis FIRST to determine if sensor was relocated
    let mobilityResult = { location_type: 'UNKNOWN', confidence: 0, reason: 'Not checked' };
    try {
        mobilityResult = await strategies.mobilityAnalysis(sensor, extendedDays);
    } catch (error) {
        mobilityResult = { location_type: 'UNKNOWN', confidence: 0, reason: `Error: ${error.message}` };
    }
    results.mobilityAnalysis = mobilityResult;

    // For RELOCATED sensors, get current location and filter data to that location
    // For MOBILE/PORTABLE, use all data (variance shows movement patterns)
    // For FIXED, use all data (no movement to worry about)
    let currentLocation = null;
    const useLocationFilter = mobilityResult.location_type === 'RELOCATED';

    if (useLocationFilter) {
        try {
            currentLocation = await getCurrentLocation(sensor.device_id);
        } catch (error) {
            // Fall back to sensor's stored location
            currentLocation = { latitude: sensor.latitude, longitude: sensor.longitude };
        }
    }

    // Run variance analysis (with location filter for RELOCATED)
    try {
        if (useLocationFilter && currentLocation) {
            // Use location-filtered stats for relocated sensors
            const stats = await getTemperatureStatsAtLocation(
                sensor.device_id,
                parseFloat(currentLocation.latitude),
                parseFloat(currentLocation.longitude),
                extendedDays
            );
            if (stats && stats.reading_count >= 24) {
                const variance = parseFloat(stats.temp_variance);
                const stddev = parseFloat(stats.temp_stddev);
                const range = parseFloat(stats.temp_range);

                // Simplified scoring for location-filtered data
                let indoorScore = 0;
                if (variance < THRESHOLDS.INDOOR_MAX_VARIANCE) indoorScore += 0.4;
                if (range < THRESHOLDS.INDOOR_MAX_RANGE) indoorScore += 0.3;
                if (stddev < 2) indoorScore += 0.3;

                let outdoorScore = 0;
                if (variance > THRESHOLDS.OUTDOOR_MIN_VARIANCE) outdoorScore += 0.4;
                if (range > THRESHOLDS.OUTDOOR_MIN_RANGE) outdoorScore += 0.3;
                if (stddev > 4) outdoorScore += 0.3;

                const classification = indoorScore > outdoorScore ? 'INDOOR' : 'OUTDOOR';
                const confidence = Math.abs(indoorScore - outdoorScore);

                results.varianceAnalysis = {
                    classification,
                    confidence: Math.min(confidence, 1),
                    reason: `Variance: ${variance.toFixed(2)}, Range: ${range.toFixed(1)}°C (at current location)`,
                    stats: { variance, range, stddev }
                };
            } else {
                results.varianceAnalysis = { score: 0, confidence: 0, reason: 'Insufficient data at current location' };
            }
        } else {
            results.varianceAnalysis = await strategies.varianceAnalysis(sensor, extendedDays);
        }
    } catch (error) {
        results.varianceAnalysis = { score: 0, confidence: 0, reason: `Error: ${error.message}` };
    }

    // Run temperature range analysis (with location filter for RELOCATED)
    try {
        if (useLocationFilter && currentLocation) {
            const stats = await getTemperatureStatsAtLocation(
                sensor.device_id,
                parseFloat(currentLocation.latitude),
                parseFloat(currentLocation.longitude),
                extendedDays
            );
            if (stats && stats.reading_count >= 24) {
                const minTemp = parseFloat(stats.min_temp);
                const maxTemp = parseFloat(stats.max_temp);
                const avgTemp = parseFloat(stats.avg_temp);

                const staysWarm = minTemp >= THRESHOLDS.INDOOR_MIN_TEMP;
                const staysCool = maxTemp <= THRESHOLDS.INDOOR_MAX_TEMP;
                const comfortableAvg = avgTemp >= 15 && avgTemp <= 28;

                let indoorScore = 0;
                if (staysWarm) indoorScore += 0.35;
                if (staysCool) indoorScore += 0.35;
                if (comfortableAvg) indoorScore += 0.3;

                let outdoorScore = 0;
                if (minTemp < 5) outdoorScore += 0.4;
                if (maxTemp > 38) outdoorScore += 0.3;
                if (maxTemp - minTemp > 20) outdoorScore += 0.3;

                const classification = indoorScore > outdoorScore ? 'INDOOR' : 'OUTDOOR';
                const confidence = Math.abs(indoorScore - outdoorScore);

                results.temperatureRange = {
                    classification,
                    confidence: Math.min(confidence, 1),
                    reason: `Range: ${minTemp.toFixed(1)}°C to ${maxTemp.toFixed(1)}°C (at current location)`,
                    stats: { minTemp, maxTemp, avgTemp }
                };
            } else {
                results.temperatureRange = { score: 0, confidence: 0, reason: 'Insufficient data at current location' };
            }
        } else {
            results.temperatureRange = await strategies.temperatureRange(sensor, extendedDays);
        }
    } catch (error) {
        results.temperatureRange = { score: 0, confidence: 0, reason: `Error: ${error.message}` };
    }

    // Fetch weather and sensor data ONCE for both strategies
    const lat = parseFloat(sensor.latitude);
    const lng = parseFloat(sensor.longitude);
    let weatherData = null;
    let sensorHourlyData = null;

    if (lat && lng && lat !== 0 && lng !== 0) {
        try {
            // Fetch sensor hourly data
            sensorHourlyData = await getHourlyTemperatures(sensor.device_id, days);
        } catch (error) {
            console.error(`Failed to fetch sensor data for ${sensor.node_name || sensor.device_id}: ${error.message}`);
        }

        try {
            // Fetch weather data once
            weatherData = await getHistoricalTemperature(lat, lng, days);
        } catch (error) {
            console.error(`Failed to fetch weather for ${lat.toFixed(2)}, ${lng.toFixed(2)}: ${error.message}`);
        }
    }

    // Run weather correlation with pre-fetched data
    try {
        results.weatherCorrelation = await strategies.weatherCorrelation(sensor, days, weatherData, sensorHourlyData);
    } catch (error) {
        results.weatherCorrelation = { score: 0, confidence: 0, reason: `Error: ${error.message}` };
    }

    // Run device temperature detection with pre-fetched data
    let deviceTempResult = { suspicious: false, confidence: 0, reason: 'Not checked' };
    try {
        deviceTempResult = await strategies.deviceTempDetection(sensor, days, weatherData, sensorHourlyData);
    } catch (error) {
        deviceTempResult = { suspicious: false, confidence: 0, reason: `Error: ${error.message}` };
    }
    results.deviceTempDetection = deviceTempResult;

    // Check if weather data was available - required for accurate classification
    const weatherDataAvailable = results.weatherCorrelation?.confidence > 0 ||
        (results.weatherCorrelation?.reason !== 'Insufficient weather data' &&
         results.weatherCorrelation?.reason !== 'No location data' &&
         !results.weatherCorrelation?.reason?.includes('Error'));

    const deviceTempDataAvailable = results.deviceTempDetection?.reason !== 'Insufficient weather data' &&
        results.deviceTempDetection?.reason !== 'No location data' &&
        !results.deviceTempDetection?.reason?.includes('Error');

    // If weather data failed, we can't accurately classify - variance alone is not enough
    // because we don't know what the local weather actually is
    if (!weatherDataAvailable) {
        // Return UNKNOWN but preserve the strategy results for debugging
        return {
            device_id: sensor.device_id,
            node_name: sensor.node_name,
            current_deployment_type: sensor.current_deployment_type || 'UNKNOWN',
            inferred_deployment_type: 'UNKNOWN',
            deployment_confidence: 0,
            relocated: mobilityResult.location_type === 'RELOCATED',
            strategies: results,
            needs_review: false, // Don't flag for review if we simply couldn't get weather data
            suspicious_reading: false,
            suspicious_reason: null,
            weather_api_failed: true,
            weather_api_failure_reason: results.weatherCorrelation?.reason || 'Unknown'
        };
    }

    // Combine results with weighted voting
    const weights = {
        varianceAnalysis: 0.35,
        weatherCorrelation: 0.40,
        temperatureRange: 0.25
    };

    const votes = { INDOOR: 0, OUTDOOR: 0 };
    let totalWeight = 0;

    for (const [name, result] of Object.entries(results)) {
        if (result.classification && result.confidence > 0 && weights[name]) {
            const weight = weights[name] * result.confidence;
            if (votes[result.classification] !== undefined) {
                votes[result.classification] += weight;
                totalWeight += weight;
            }
        }
    }

    // Determine final classification
    let finalClassification = 'UNKNOWN';
    let finalConfidence = 0;

    if (totalWeight > 0) {
        const sorted = Object.entries(votes).sort((a, b) => b[1] - a[1]);
        finalClassification = sorted[0][0];
        finalConfidence = sorted[0][1] / totalWeight;

        // If votes are close, leave as UNKNOWN (can't determine)
        if (sorted.length > 1 && sorted[0][1] - sorted[1][1] < 0.1) {
            finalClassification = 'UNKNOWN';
            finalConfidence = 0;
        }
    }

    // Get stats for DEVICE detection and suspicious reading logic
    const varianceStats = results.varianceAnalysis?.stats;
    const tempRangeStats = results.temperatureRange?.stats;
    const avgTemp = tempRangeStats?.avgTemp;
    const variance = varianceStats?.variance;

    // Check for DEVICE classification (measuring chip/hardware temp, not ambient)
    // Criteria: super stable + suspicious + running hot (avg > 30°C)
    const isDeviceTemp = deviceTempResult.suspicious &&
        deviceTempResult.anomalyType === 'ABOVE_AMBIENT' &&
        variance !== undefined && variance < 1.5 &&
        avgTemp !== undefined && avgTemp > 30;

    if (isDeviceTemp) {
        finalClassification = 'DEVICE';
        finalConfidence = 0.95; // High confidence when all criteria match
    }

    // Check for NON_AIR classification (measuring something other than air - pipe, water, soil, etc.)
    // Criteria: consistently below ambient temperature
    const isBelowAmbient = deviceTempResult.suspicious &&
        deviceTempResult.anomalyType === 'BELOW_AMBIENT';

    if (isBelowAmbient) {
        finalClassification = 'NON_AIR';
        finalConfidence = deviceTempResult.confidence;
    }

    // Check for MOBILE/PORTABLE - these are deployment types that override INDOOR/OUTDOOR
    // A mobile sensor is a deployment classification, not a separate dimension
    const mobilityType = mobilityResult.location_type;
    const isRelocated = mobilityType === 'RELOCATED';
    if (mobilityType === 'MOBILE' || mobilityType === 'PORTABLE') {
        finalClassification = mobilityType;
        finalConfidence = mobilityResult.confidence;
    }

    // Determine if reading is suspicious (not measuring ambient air temp)
    // Flag as suspicious if:
    // 1. Classified as OUTDOOR but running above ambient (possible device temp or enclosure heat)
    // 2. Classified as DEVICE (by definition suspicious - measuring chip temp)
    // 3. Classified as NON_AIR (by definition suspicious - measuring something cold)
    // 4. Hot (>30°C) AND stable (variance < 3) - likely measuring chip temp not ambient
    // Indoor sensors in warm environments (sunrooms, tropical locations) should NOT be flagged
    const rawSuspicious = deviceTempResult.suspicious;
    const isStableAndHot = avgTemp !== undefined && avgTemp > 30 &&
        variance !== undefined && variance < 3;
    const suspiciousReading = rawSuspicious && (
        finalClassification === 'OUTDOOR' ||
        finalClassification === 'DEVICE' ||
        finalClassification === 'NON_AIR' ||
        isStableAndHot
    );
    const suspiciousReason = suspiciousReading ? deviceTempResult.reason : null;

    return {
        device_id: sensor.device_id,
        node_name: sensor.node_name,
        current_deployment_type: sensor.current_deployment_type || 'UNKNOWN',
        // Inferred values - these are educated guesses, not confirmed facts
        inferred_deployment_type: finalClassification,
        deployment_confidence: finalConfidence,
        relocated: isRelocated, // Flag if sensor has moved to a new location
        strategies: results,
        needs_review: finalClassification !== (sensor.current_deployment_type || 'UNKNOWN'),
        suspicious_reading: suspiciousReading,
        suspicious_reason: suspiciousReason
    };
}

/**
 * Classify all sensors and generate a report
 */
export async function classifyAllSensors(sensors, days = 7) {
    const results = [];
    const total = sensors.length;

    for (let i = 0; i < sensors.length; i++) {
        const sensor = sensors[i];
        const result = await classifySensor(sensor, days);
        results.push(result);

        // Print result line-by-line
        const name = (sensor.node_name || sensor.device_id).padEnd(30).slice(0, 30);
        const deployment = result.inferred_deployment_type.padEnd(8);
        const confidence = `${(result.deployment_confidence * 100).toFixed(0)}%`.padStart(4);
        const relocated = result.relocated ? ' [RELOCATED]' : '';

        // Build reason from strategies
        let reason = '';
        if (result.inferred_deployment_type === 'UNKNOWN') {
            // Show why it failed
            const failReasons = [];
            for (const [stratName, strat] of Object.entries(result.strategies)) {
                if (strat.confidence === 0 && strat.reason) {
                    failReasons.push(strat.reason);
                }
            }
            reason = failReasons[0] || 'No data';
        } else {
            // Show the primary reason for classification
            const dominated = Object.entries(result.strategies)
                .filter(([name, s]) => s.classification === result.inferred_deployment_type && s.confidence > 0)
                .sort((a, b) => b[1].confidence - a[1].confidence)[0];
            if (dominated) {
                reason = dominated[1].reason;
            }
        }

        // Add weather vs sensor comparison if available
        const weatherStats = result.strategies.deviceTempDetection?.stats;
        let tempComparison = '';
        if (weatherStats?.weatherAvg !== undefined && weatherStats?.sensorAvg !== undefined) {
            const weatherAvg = weatherStats.weatherAvg.toFixed(1);
            const sensorAvg = weatherStats.sensorAvg.toFixed(1);
            tempComparison = ` [Weather: ${weatherAvg}°C → Sensor: ${sensorAvg}°C]`;
        }

        const suspicious = result.suspicious_reading ? ' ⚠️ SUSPICIOUS' : '';
        const rowCount = sensor.reading_count ? `(${sensor.reading_count} rows)` : '';

        console.log(`[${i + 1}/${total}] ${name} → ${deployment} ${confidence} ${rowCount}${relocated} ${reason}${tempComparison}${suspicious}`);

        // Rate limit API calls
        await new Promise(resolve => setTimeout(resolve, 500));
    }

    // Print summary
    const deploymentCounts = {};
    let suspiciousCount = 0;
    let relocatedCount = 0;

    for (const r of results) {
        deploymentCounts[r.inferred_deployment_type] = (deploymentCounts[r.inferred_deployment_type] || 0) + 1;
        if (r.suspicious_reading) suspiciousCount++;
        if (r.relocated) relocatedCount++;
    }

    console.log('\n' + '-'.repeat(60));
    console.log('CLASSIFICATION SUMMARY');
    console.log('-'.repeat(60));
    for (const [type, count] of Object.entries(deploymentCounts).sort((a, b) => b[1] - a[1])) {
        console.log(`  ${type.padEnd(10)} ${count}`);
    }
    if (relocatedCount > 0) {
        console.log(`\n  Relocated sensors: ${relocatedCount}`);
    }
    if (suspiciousCount > 0) {
        console.log(`  ⚠️  Suspicious: ${suspiciousCount}`);
    }
    console.log('-'.repeat(60) + '\n');

    return results;
}

export { THRESHOLDS };
