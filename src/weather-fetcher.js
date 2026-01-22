/**
 * Fetches historical weather data from Open-Meteo API (free, no API key needed)
 * https://open-meteo.com/
 */

const OPEN_METEO_BASE = 'https://api.open-meteo.com/v1';
const MAX_RETRIES = 3;
const INITIAL_RETRY_DELAY_MS = 1000;

/**
 * Sleep for a specified duration
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Get historical hourly temperature for a location
 * @param {number} lat - Latitude
 * @param {number} lng - Longitude
 * @param {number} days - Number of days of history (max 92 for free tier)
 * @returns {Array<{time: string, temperature: number}>}
 */
export async function getHistoricalTemperature(lat, lng, days = 7) {
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - days);

    const formatDate = (d) => d.toISOString().split('T')[0];

    // Use 2 decimal places (~1km precision) - more likely to match API grid
    // and we don't need meter-level precision for weather comparison
    const url = `${OPEN_METEO_BASE}/forecast?` + new URLSearchParams({
        latitude: lat.toFixed(2),
        longitude: lng.toFixed(2),
        hourly: 'temperature_2m',
        start_date: formatDate(startDate),
        end_date: formatDate(endDate),
        timezone: 'UTC'  // Match sensor data which is stored in UTC
    });

    let lastError = null;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            const response = await fetch(url, { timeout: 10000 });
            if (!response.ok) {
                const errorText = await response.text().catch(() => 'Unknown error');
                // Don't retry on 4xx errors (client errors), except 429 (rate limit)
                if (response.status >= 400 && response.status < 500 && response.status !== 429) {
                    console.error(`Open-Meteo API error for ${lat.toFixed(2)}, ${lng.toFixed(2)}: ${response.status} - ${errorText}`);
                    return [];
                }
                throw new Error(`HTTP ${response.status}: ${errorText}`);
            }

            const data = await response.json();

            // Check for API-level errors
            if (data.error) {
                console.error(`Open-Meteo API returned error for ${lat.toFixed(2)}, ${lng.toFixed(2)}: ${data.reason || data.error}`);
                return [];
            }

            // Transform to array of {time, temperature}
            const times = data.hourly?.time || [];
            const temps = data.hourly?.temperature_2m || [];

            if (times.length === 0) {
                console.error(`Open-Meteo API returned no data for ${lat.toFixed(2)}, ${lng.toFixed(2)}`);
                return [];
            }

            return times.map((time, i) => ({
                time: new Date(time),
                temperature: temps[i]
            })).filter(r => r.temperature !== null);

        } catch (error) {
            lastError = error;
            if (attempt < MAX_RETRIES) {
                const delay = INITIAL_RETRY_DELAY_MS * Math.pow(2, attempt - 1);
                await sleep(delay);
            }
        }
    }

    console.error(`Failed to fetch weather for ${lat.toFixed(2)}, ${lng.toFixed(2)} after ${MAX_RETRIES} attempts: ${lastError?.message}`);
    return [];
}

/**
 * Calculate correlation coefficient between two arrays
 * @param {number[]} x
 * @param {number[]} y
 * @returns {number} Pearson correlation (-1 to 1)
 */
export function calculateCorrelation(x, y) {
    if (x.length !== y.length || x.length < 3) {
        return 0;
    }

    const n = x.length;
    const sumX = x.reduce((a, b) => a + b, 0);
    const sumY = y.reduce((a, b) => a + b, 0);
    const sumXY = x.reduce((acc, xi, i) => acc + xi * y[i], 0);
    const sumX2 = x.reduce((acc, xi) => acc + xi * xi, 0);
    const sumY2 = y.reduce((acc, yi) => acc + yi * yi, 0);

    const numerator = n * sumXY - sumX * sumY;
    const denominator = Math.sqrt(
        (n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY)
    );

    if (denominator === 0) return 0;

    return numerator / denominator;
}

/**
 * Compare sensor readings to weather data and calculate correlation
 * @param {Array<{hour: Date, avg_temp: number}>} sensorData - Hourly sensor temps
 * @param {Array<{time: Date, temperature: number}>} weatherData - Hourly weather temps
 * @returns {{correlation: number, matchedHours: number}}
 */
export function compareSensorToWeather(sensorData, weatherData) {
    // Create lookup map for weather data by hour
    const weatherMap = new Map();
    for (const w of weatherData) {
        const hourKey = new Date(w.time).toISOString().slice(0, 13); // YYYY-MM-DDTHH
        weatherMap.set(hourKey, w.temperature);
    }

    // Match sensor hours to weather hours
    const sensorTemps = [];
    const weatherTemps = [];

    for (const s of sensorData) {
        const hourKey = new Date(s.hour).toISOString().slice(0, 13);
        const weatherTemp = weatherMap.get(hourKey);

        if (weatherTemp !== undefined) {
            sensorTemps.push(parseFloat(s.avg_temp));
            weatherTemps.push(weatherTemp);
        }
    }

    const correlation = calculateCorrelation(sensorTemps, weatherTemps);

    return {
        correlation,
        matchedHours: sensorTemps.length,
        sensorTemps,
        weatherTemps
    };
}

/**
 * Calculate detailed comparison stats between sensor and weather
 * Used to detect device temperature sensors (consistently above ambient)
 */
export function calculateWeatherComparisonStats(sensorData, weatherData) {
    const { correlation, matchedHours, sensorTemps, weatherTemps } =
        compareSensorToWeather(sensorData, weatherData);

    if (matchedHours < 24) {
        return null;
    }

    // Calculate statistics
    const sensorMin = Math.min(...sensorTemps);
    const sensorMax = Math.max(...sensorTemps);
    const sensorAvg = sensorTemps.reduce((a, b) => a + b, 0) / sensorTemps.length;

    const weatherMin = Math.min(...weatherTemps);
    const weatherMax = Math.max(...weatherTemps);
    const weatherAvg = weatherTemps.reduce((a, b) => a + b, 0) / weatherTemps.length;

    // Calculate differences
    const avgDiff = sensorAvg - weatherAvg;
    const minDiff = sensorMin - weatherMin;
    const maxDiff = sensorMax - weatherMax;

    // Calculate mean absolute difference per hour
    const hourlyDiffs = sensorTemps.map((s, i) => s - weatherTemps[i]);
    const meanAbsDiff = hourlyDiffs.reduce((a, b) => a + Math.abs(b), 0) / hourlyDiffs.length;

    return {
        correlation,
        matchedHours,
        sensor: { min: sensorMin, max: sensorMax, avg: sensorAvg },
        weather: { min: weatherMin, max: weatherMax, avg: weatherAvg },
        diff: { avg: avgDiff, min: minDiff, max: maxDiff, meanAbs: meanAbsDiff }
    };
}
