import { createClient } from '@clickhouse/client';
import fs from 'fs';

const tlsEnabled = process.env.TLS_ENABLED === 'true';
const defaultUrl = tlsEnabled ? 'https://clickhouse:8443' : 'http://localhost:8123';
const clickhouseUrl = tlsEnabled
    ? (process.env.CLICKHOUSE_HOST || defaultUrl).replace('http://', 'https://').replace(':8123', ':8443')
    : (process.env.CLICKHOUSE_HOST || defaultUrl);

const clientOpts = {
    url: clickhouseUrl,
    username: process.env.CLICKHOUSE_USER || 'wesense',
    password: process.env.CLICKHOUSE_PASSWORD || '',
    database: process.env.CLICKHOUSE_DATABASE || 'wesense'
};

if (tlsEnabled) {
    const caFile = process.env.TLS_CA_CERTFILE;
    if (caFile && fs.existsSync(caFile)) {
        clientOpts.tls = { ca_cert: fs.readFileSync(caFile) };
    }
}

const client = createClient(clientOpts);

// Minimum temperature readings required for classification
// - Mobility analysis needs 10+ readings
// - Weather correlation needs 24+ matched hours
// - Set to 24 to ensure meaningful weather comparison
const MIN_READINGS_FOR_CLASSIFICATION = 24;

/**
 * Get all sensors with their locations and current deployment type
 * Only returns sensors that need classification (Meshtastic, HomeAssistant)
 * WeSense sensors are excluded - they have calibrated sensors and don't need classification
 */
export async function getSensors() {
    const query = `
        SELECT
            device_id,
            any(node_name) as node_name,
            any(latitude) as latitude,
            any(longitude) as longitude,
            any(deployment_type) as current_deployment_type,
            any(data_source) as sensor_data_source,
            count() as reading_count,
            countIf(reading_type = 'temperature') as temp_reading_count,
            min(timestamp) as first_seen,
            max(timestamp) as last_seen
        FROM wesense.sensor_readings
        WHERE timestamp > now() - INTERVAL 90 DAY
          AND (upper(data_source) LIKE 'MESHTASTIC%' OR upper(data_source) = 'HOMEASSISTANT')
        GROUP BY device_id
        HAVING latitude != 0 AND longitude != 0
          AND temp_reading_count >= ${MIN_READINGS_FOR_CLASSIFICATION}
        ORDER BY reading_count DESC
    `;

    const result = await client.query({ query, format: 'JSONEachRow' });
    return result.json();
}

/**
 * Get temperature readings for a sensor over a time period
 */
export async function getTemperatureReadings(deviceId, days = 7) {
    const query = `
        SELECT
            timestamp,
            value as temperature
        FROM wesense.sensor_readings
        WHERE device_id = {deviceId:String}
          AND reading_type = 'temperature'
          AND timestamp > now() - INTERVAL {days:UInt32} DAY
        ORDER BY timestamp
    `;

    const result = await client.query({
        query,
        query_params: { deviceId, days },
        format: 'JSONEachRow'
    });
    return result.json();
}

/**
 * Get temperature statistics for a sensor
 */
export async function getTemperatureStats(deviceId, days = 7) {
    const query = `
        SELECT
            device_id,
            count() as reading_count,
            avg(value) as avg_temp,
            min(value) as min_temp,
            max(value) as max_temp,
            max(value) - min(value) as temp_range,
            stddevPop(value) as temp_stddev,
            varPop(value) as temp_variance
        FROM wesense.sensor_readings
        WHERE device_id = {deviceId:String}
          AND reading_type = 'temperature'
          AND timestamp > now() - INTERVAL {days:UInt32} DAY
          AND value > -50 AND value < 60
        GROUP BY device_id
    `;

    const result = await client.query({
        query,
        query_params: { deviceId, days },
        format: 'JSONEachRow'
    });
    const rows = await result.json();
    return rows[0] || null;
}

/**
 * Get hourly temperature averages for correlation with weather data
 */
export async function getHourlyTemperatures(deviceId, days = 7) {
    const query = `
        SELECT
            toStartOfHour(timestamp) as hour,
            avg(value) as avg_temp,
            count() as readings
        FROM wesense.sensor_readings
        WHERE device_id = {deviceId:String}
          AND reading_type = 'temperature'
          AND timestamp > now() - INTERVAL {days:UInt32} DAY
          AND value > -50 AND value < 60
        GROUP BY hour
        ORDER BY hour
    `;

    const result = await client.query({
        query,
        query_params: { deviceId, days },
        format: 'JSONEachRow'
    });
    return result.json();
}

/**
 * Update deployment type for a sensor
 */
export async function updateDeploymentType(deviceId, deploymentType) {
    // Note: This would update a separate classification table
    // For now, just log - actual implementation depends on schema design
    console.log(`Would update ${deviceId} to ${deploymentType}`);
}

/**
 * Get location mobility stats for a sensor
 * Returns the geographic range of positions to detect if sensor is mobile
 */
/**
 * Get temperature statistics filtered to a specific location (for relocated sensors)
 * Uses a tolerance of ~1km to account for GPS drift
 */
export async function getTemperatureStatsAtLocation(deviceId, lat, lng, days = 90) {
    const tolerance = 0.01; // ~1km tolerance for GPS drift
    const query = `
        SELECT
            device_id,
            count() as reading_count,
            avg(value) as avg_temp,
            min(value) as min_temp,
            max(value) as max_temp,
            max(value) - min(value) as temp_range,
            stddevPop(value) as temp_stddev,
            varPop(value) as temp_variance
        FROM wesense.sensor_readings
        WHERE device_id = {deviceId:String}
          AND reading_type = 'temperature'
          AND timestamp > now() - INTERVAL {days:UInt32} DAY
          AND value > -50 AND value < 60
          AND abs(latitude - {lat:Float64}) < {tolerance:Float64}
          AND abs(longitude - {lng:Float64}) < {tolerance:Float64}
        GROUP BY device_id
    `;

    const result = await client.query({
        query,
        query_params: { deviceId, days, lat, lng, tolerance },
        format: 'JSONEachRow'
    });
    const rows = await result.json();
    return rows[0] || null;
}

/**
 * Get current (most recent) location for a sensor
 */
export async function getCurrentLocation(deviceId) {
    const query = `
        SELECT
            latitude,
            longitude
        FROM wesense.sensor_readings
        WHERE device_id = {deviceId:String}
          AND latitude != 0 AND longitude != 0
        ORDER BY timestamp DESC
        LIMIT 1
    `;

    const result = await client.query({
        query,
        query_params: { deviceId },
        format: 'JSONEachRow'
    });
    const rows = await result.json();
    return rows[0] || null;
}

export async function getLocationMobility(deviceId, days = 7) {
    const query = `
        SELECT
            device_id,
            count() as reading_count,
            count(DISTINCT round(latitude, 3), round(longitude, 3)) as unique_locations,
            min(latitude) as min_lat,
            max(latitude) as max_lat,
            min(longitude) as min_lon,
            max(longitude) as max_lon,
            max(latitude) - min(latitude) as lat_range,
            max(longitude) - min(longitude) as lon_range
        FROM wesense.sensor_readings
        WHERE device_id = {deviceId:String}
          AND timestamp > now() - INTERVAL {days:UInt32} DAY
          AND latitude != 0 AND longitude != 0
        GROUP BY device_id
    `;

    const result = await client.query({
        query,
        query_params: { deviceId, days },
        format: 'JSONEachRow'
    });
    const rows = await result.json();

    if (!rows[0]) return null;

    const row = rows[0];
    // Approximate distance in km using simple pythagorean (good enough for detection)
    // 111 km per degree latitude, longitude varies by latitude
    const latKm = parseFloat(row.lat_range) * 111;
    const avgLat = (parseFloat(row.min_lat) + parseFloat(row.max_lat)) / 2;
    const lonKm = parseFloat(row.lon_range) * 111 * Math.cos(avgLat * Math.PI / 180);
    const rangeKm = Math.sqrt(latKm * latKm + lonKm * lonKm);

    return {
        reading_count: parseInt(row.reading_count),
        unique_locations: parseInt(row.unique_locations),
        range_km: rangeKm,
        lat_range: parseFloat(row.lat_range),
        lon_range: parseFloat(row.lon_range)
    };
}

export { client };
