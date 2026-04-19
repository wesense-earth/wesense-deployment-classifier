#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { getSensors, client } from './clickhouse-client.js';
import { classifySensor, classifyAllSensors, THRESHOLDS } from './classifier.js';
import { ClassificationState } from './classification-state.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = path.join(__dirname, '..', 'reports');
const ARCHIVE_REPLICATOR_URL = (() => {
    let url = process.env.ARCHIVE_REPLICATOR_URL || '';
    if (process.env.TLS_ENABLED === 'true' && url) {
        url = url.replace('http://', 'https://');
    }
    return url;
})();

// Check if running in scheduler mode
const SCHEDULER_MODE = process.env.CLASSIFIER_MODE === 'scheduler';
const SCHEDULE = process.env.CLASSIFIER_SCHEDULE || '0 */12 * * *'; // Default: every 12 hours
const DRY_RUN = process.env.DRY_RUN === 'true' || process.env.DRY_RUN === '1';
const RUN_ON_STARTUP = process.env.RUN_ON_STARTUP === 'true' || process.env.RUN_ON_STARTUP === '1';

/**
 * Save report to JSON file
 */
function saveReport(results) {
    // Create reports directory if it doesn't exist
    if (!fs.existsSync(REPORTS_DIR)) {
        fs.mkdirSync(REPORTS_DIR, { recursive: true });
    }

    // Generate filename with timestamp
    const now = new Date();
    const timestamp = now.toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const filename = `classification-${timestamp}.json`;
    const filepath = path.join(REPORTS_DIR, filename);

    // Analyze UNKNOWN sensors
    const unknownSensors = results.filter(r => r.inferred_deployment_type === 'UNKNOWN');
    const unknownByReason = {
        insufficient_data: [],
        no_location: [],
        insufficient_sensor_data: [],
        insufficient_weather_data: [],
        not_enough_matched_hours: [],
        other: []
    };

    for (const r of unknownSensors) {
        const reasons = [];
        const strats = r.strategies;

        if (strats.varianceAnalysis) {
            const reason = strats.varianceAnalysis.reason;
            if (reason === 'Insufficient data' || strats.varianceAnalysis.confidence === 0) {
                reasons.push(`variance: ${reason}`);
            }
        }
        if (strats.weatherCorrelation) {
            const reason = strats.weatherCorrelation.reason;
            if (reason === 'No location data' || reason === 'Insufficient sensor data' ||
                reason === 'Insufficient weather data' || reason?.includes('matched hours') ||
                strats.weatherCorrelation.confidence === 0) {
                reasons.push(`weather: ${reason}`);
            }
        }
        if (strats.temperatureRange) {
            const reason = strats.temperatureRange.reason;
            if (reason === 'Insufficient data' || strats.temperatureRange.confidence === 0) {
                reasons.push(`tempRange: ${reason}`);
            }
        }

        const entry = {
            device_id: r.device_id,
            node_name: r.node_name,
            reasons: reasons
        };

        const combinedReason = reasons.join('; ') || 'Unknown';
        if (combinedReason.includes('No location')) {
            unknownByReason.no_location.push(entry);
        } else if (combinedReason.includes('Insufficient data')) {
            unknownByReason.insufficient_data.push(entry);
        } else if (combinedReason.includes('Insufficient sensor')) {
            unknownByReason.insufficient_sensor_data.push(entry);
        } else if (combinedReason.includes('Insufficient weather')) {
            unknownByReason.insufficient_weather_data.push(entry);
        } else if (combinedReason.includes('matched hours')) {
            unknownByReason.not_enough_matched_hours.push(entry);
        } else {
            unknownByReason.other.push(entry);
        }
    }

    // Count weather API failures
    const weatherApiFailed = results.filter(r => r.weather_api_failed);
    const weatherFailureReasons = {};
    for (const r of weatherApiFailed) {
        const reason = r.weather_api_failure_reason || 'Unknown';
        weatherFailureReasons[reason] = (weatherFailureReasons[reason] || 0) + 1;
    }

    // Build report object
    const report = {
        generated_at: now.toISOString(),
        note: 'All inferred values are educated guesses based on data analysis, not confirmed facts',
        summary: {
            total: results.length,
            needs_review: results.filter(r => r.needs_review).length,
            suspicious_readings: results.filter(r => r.suspicious_reading).length,
            relocated_sensors: results.filter(r => r.relocated).length,
            unknown_count: unknownSensors.length,
            weather_api_failures: weatherApiFailed.length,
            by_deployment_type: {}
        },
        suspicious_sensors: results.filter(r => r.suspicious_reading).map(r => ({
            device_id: r.device_id,
            node_name: r.node_name,
            reason: r.suspicious_reason,
            stats: r.strategies.deviceTempDetection?.stats
        })),
        mobile_sensors: results.filter(r => ['MOBILE', 'PORTABLE'].includes(r.inferred_deployment_type)).map(r => ({
            device_id: r.device_id,
            node_name: r.node_name,
            deployment_type: r.inferred_deployment_type,
            reason: r.strategies.mobilityAnalysis?.reason,
            stats: r.strategies.mobilityAnalysis?.stats
        })),
        unknown_sensors: {
            total: unknownSensors.length,
            by_reason: {
                insufficient_data: unknownByReason.insufficient_data.length,
                no_location: unknownByReason.no_location.length,
                insufficient_sensor_data: unknownByReason.insufficient_sensor_data.length,
                insufficient_weather_data: unknownByReason.insufficient_weather_data.length,
                not_enough_matched_hours: unknownByReason.not_enough_matched_hours.length,
                other: unknownByReason.other.length
            },
            details: unknownByReason
        },
        weather_api_failures: {
            total: weatherApiFailed.length,
            by_reason: weatherFailureReasons,
            sensors: weatherApiFailed.map(r => ({
                device_id: r.device_id,
                node_name: r.node_name,
                reason: r.weather_api_failure_reason,
                current_deployment_type: r.current_deployment_type
            }))
        },
        sensors: results
    };

    for (const r of results) {
        const deploymentType = r.inferred_deployment_type;
        report.summary.by_deployment_type[deploymentType] = (report.summary.by_deployment_type[deploymentType] || 0) + 1;
    }

    // Write JSON file
    fs.writeFileSync(filepath, JSON.stringify(report, null, 2));
    console.log(`\nJSON report saved to: ${filepath}`);

    // Write CSV file
    const csvFilename = `classification-${timestamp}.csv`;
    const csvFilepath = path.join(REPORTS_DIR, csvFilename);

    const csvHeader = [
        'device_id',
        'node_name',
        'inferred_deployment_type',
        'deployment_confidence',
        'relocated',
        'suspicious_reading',
        'variance',
        'sensor_avg_temp',
        'weather_avg_temp',
        'temp_diff',
        'weather_correlation',
        'classified_at'
    ].join(',');

    const escapeCSV = (value) => {
        if (value === null || value === undefined) return '';
        const str = String(value);
        if (str.includes(',') || str.includes('"') || str.includes('\n')) {
            return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
    };

    const csvRows = results.map(r => {
        const variance = r.strategies.varianceAnalysis?.stats?.variance;
        const avgTemp = r.strategies.temperatureRange?.stats?.avgTemp;
        const correlation = r.strategies.weatherCorrelation?.stats?.correlation;
        const weatherStats = r.strategies.deviceTempDetection?.stats;
        const weatherAvg = weatherStats?.weatherAvg;
        const sensorAvg = weatherStats?.sensorAvg;
        const tempDiff = weatherStats?.avgDiff;

        return [
            escapeCSV(r.device_id),
            escapeCSV(r.node_name),
            escapeCSV(r.inferred_deployment_type),
            r.deployment_confidence?.toFixed(3) || '',
            r.relocated ? 'true' : 'false',
            r.suspicious_reading ? 'true' : 'false',
            variance?.toFixed(3) || '',
            sensorAvg?.toFixed(2) || avgTemp?.toFixed(2) || '',
            weatherAvg?.toFixed(2) || '',
            tempDiff?.toFixed(2) || '',
            correlation?.toFixed(4) || '',
            now.toISOString()
        ].join(',');
    });

    const csvContent = [csvHeader, ...csvRows].join('\n');
    fs.writeFileSync(csvFilepath, csvContent);
    console.log(`CSV report saved to: ${csvFilepath}`);

    return filepath;
}

/**
 * Print classification report
 */
function printReport(results) {
    console.log('\n' + '='.repeat(80));
    console.log('INFERRED SENSOR CLASSIFICATION REPORT');
    console.log('(All classifications are educated guesses based on data analysis)');
    console.log('='.repeat(80) + '\n');

    // Summary
    const weatherApiFailed = results.filter(r => r.weather_api_failed);
    const summary = {
        total: results.length,
        needsReview: results.filter(r => r.needs_review).length,
        suspicious: results.filter(r => r.suspicious_reading).length,
        relocated: results.filter(r => r.relocated).length,
        weatherApiFailed: weatherApiFailed.length,
        byDeploymentType: {}
    };

    for (const r of results) {
        const deploymentType = r.inferred_deployment_type;
        summary.byDeploymentType[deploymentType] = (summary.byDeploymentType[deploymentType] || 0) + 1;
    }

    console.log('SUMMARY:');
    console.log(`  Total sensors analyzed: ${summary.total}`);
    console.log(`  Needing review: ${summary.needsReview}`);
    console.log(`  Suspicious readings (likely device temp): ${summary.suspicious}`);
    console.log(`  Relocated sensors: ${summary.relocated}`);
    if (summary.weatherApiFailed > 0) {
        console.log(`  Weather API failures: ${summary.weatherApiFailed} (not classified)`);
    }
    console.log('  By deployment type:');
    for (const [type, count] of Object.entries(summary.byDeploymentType)) {
        console.log(`    ${type}: ${count}`);
    }

    // Sensors needing review (classification differs from current)
    const needsReview = results.filter(r => r.needs_review);
    if (needsReview.length > 0) {
        console.log('\n' + '-'.repeat(80));
        console.log('SENSORS NEEDING REVIEW:');
        console.log('-'.repeat(80));

        for (const r of needsReview) {
            console.log(`\n${r.node_name || r.device_id}`);
            console.log(`  Device ID: ${r.device_id}`);
            console.log(`  Current: ${r.current_deployment_type} → Inferred: ${r.inferred_deployment_type}`);
            console.log(`  Confidence: ${(r.deployment_confidence * 100).toFixed(0)}%${r.relocated ? ' [RELOCATED]' : ''}`);
            console.log('  Strategy results:');
            for (const [name, result] of Object.entries(r.strategies)) {
                if (result.classification) {
                    console.log(`    ${name}: ${result.classification} (${(result.confidence * 100).toFixed(0)}%) - ${result.reason}`);
                }
            }
            if (r.suspicious_reading) {
                console.log(`  ⚠️  SUSPICIOUS: ${r.suspicious_reason}`);
            }
        }
    }

    // Suspicious sensors (likely reporting device temperature, not air temperature)
    const suspiciousSensors = results.filter(r => r.suspicious_reading);
    if (suspiciousSensors.length > 0) {
        console.log('\n' + '-'.repeat(80));
        console.log('⚠️  SUSPICIOUS READINGS (likely device/chip temperature, not air):');
        console.log('-'.repeat(80));
        console.log('These sensors report temperatures consistently above local weather.');
        console.log('They may be measuring internal device temperature rather than ambient air.\n');

        for (const r of suspiciousSensors) {
            const stats = r.strategies.deviceTempDetection?.stats || {};
            console.log(`${r.node_name || r.device_id}`);
            console.log(`  Device ID: ${r.device_id}`);
            console.log(`  Reason: ${r.suspicious_reason}`);
            if (stats.sensorAvg && stats.weatherAvg) {
                console.log(`  Sensor avg: ${stats.sensorAvg.toFixed(1)}°C vs Weather avg: ${stats.weatherAvg.toFixed(1)}°C (diff: +${stats.avgDiff.toFixed(1)}°C)`);
            }
            console.log('');
        }
    }

    // High confidence indoor sensors
    const indoorSensors = results.filter(r =>
        r.inferred_deployment_type === 'INDOOR' && r.deployment_confidence > 0.6
    );
    if (indoorSensors.length > 0) {
        console.log('\n' + '-'.repeat(80));
        console.log('HIGH CONFIDENCE INDOOR SENSORS:');
        console.log('-'.repeat(80));

        for (const r of indoorSensors) {
            const variance = r.strategies.varianceAnalysis?.stats?.variance?.toFixed(2) || 'N/A';
            const correlation = r.strategies.weatherCorrelation?.stats?.correlation?.toFixed(3) || 'N/A';
            const relocated = r.relocated ? ' [RELOCATED]' : '';
            console.log(`  ${r.node_name || r.device_id}: ${(r.deployment_confidence * 100).toFixed(0)}% confidence (variance: ${variance}, weather corr: ${correlation})${relocated}`);
        }
    }

    // High confidence outdoor sensors
    const outdoorSensors = results.filter(r =>
        r.inferred_deployment_type === 'OUTDOOR' && r.deployment_confidence > 0.6
    );
    if (outdoorSensors.length > 0) {
        console.log('\n' + '-'.repeat(80));
        console.log('HIGH CONFIDENCE OUTDOOR SENSORS:');
        console.log('-'.repeat(80));

        for (const r of outdoorSensors) {
            const variance = r.strategies.varianceAnalysis?.stats?.variance?.toFixed(2) || 'N/A';
            const correlation = r.strategies.weatherCorrelation?.stats?.correlation?.toFixed(3) || 'N/A';
            const relocated = r.relocated ? ' [RELOCATED]' : '';
            console.log(`  ${r.node_name || r.device_id}: ${(r.deployment_confidence * 100).toFixed(0)}% confidence (variance: ${variance}, weather corr: ${correlation})${relocated}`);
        }
    }

    // Mobile/portable sensors
    const mobileSensors = results.filter(r =>
        ['MOBILE', 'PORTABLE'].includes(r.inferred_deployment_type)
    );
    if (mobileSensors.length > 0) {
        console.log('\n' + '-'.repeat(80));
        console.log('MOBILE/PORTABLE SENSORS:');
        console.log('-'.repeat(80));

        for (const r of mobileSensors) {
            const stats = r.strategies.mobilityAnalysis?.stats;
            const range = stats?.range_km?.toFixed(1) || 'N/A';
            const locations = stats?.unique_locations || 'N/A';
            console.log(`  ${r.node_name || r.device_id}: ${r.inferred_deployment_type} (${range}km range, ${locations} locations)`);
        }
    }

    // UNKNOWN sensors analysis
    const unknownSensors = results.filter(r => r.inferred_deployment_type === 'UNKNOWN');
    if (unknownSensors.length > 0) {
        console.log('\n' + '-'.repeat(80));
        console.log('UNKNOWN SENSORS - CLASSIFICATION FAILED:');
        console.log('-'.repeat(80));
        console.log('These sensors could not be classified. Reasons breakdown:\n');

        // Categorize reasons
        const reasonCounts = {
            'Insufficient data': [],
            'No location data': [],
            'Insufficient sensor data': [],
            'Insufficient weather data': [],
            'Not enough matched hours': [],
            'Other/Error': []
        };

        for (const r of unknownSensors) {
            const reasons = [];
            const strats = r.strategies;

            // Check each strategy for its failure reason
            if (strats.varianceAnalysis) {
                const reason = strats.varianceAnalysis.reason;
                if (reason === 'Insufficient data' || strats.varianceAnalysis.confidence === 0) {
                    reasons.push(`variance: ${reason}`);
                }
            }
            if (strats.weatherCorrelation) {
                const reason = strats.weatherCorrelation.reason;
                if (reason === 'No location data' || reason === 'Insufficient sensor data' ||
                    reason === 'Insufficient weather data' || reason?.includes('matched hours') ||
                    strats.weatherCorrelation.confidence === 0) {
                    reasons.push(`weather: ${reason}`);
                }
            }
            if (strats.temperatureRange) {
                const reason = strats.temperatureRange.reason;
                if (reason === 'Insufficient data' || strats.temperatureRange.confidence === 0) {
                    reasons.push(`tempRange: ${reason}`);
                }
            }

            // Categorize into buckets
            const combinedReason = reasons.join('; ') || 'Unknown';
            if (combinedReason.includes('No location')) {
                reasonCounts['No location data'].push({ sensor: r, reasons });
            } else if (combinedReason.includes('Insufficient data')) {
                reasonCounts['Insufficient data'].push({ sensor: r, reasons });
            } else if (combinedReason.includes('Insufficient sensor')) {
                reasonCounts['Insufficient sensor data'].push({ sensor: r, reasons });
            } else if (combinedReason.includes('Insufficient weather')) {
                reasonCounts['Insufficient weather data'].push({ sensor: r, reasons });
            } else if (combinedReason.includes('matched hours')) {
                reasonCounts['Not enough matched hours'].push({ sensor: r, reasons });
            } else {
                reasonCounts['Other/Error'].push({ sensor: r, reasons });
            }
        }

        // Print summary by reason
        for (const [reason, sensors] of Object.entries(reasonCounts)) {
            if (sensors.length > 0) {
                console.log(`  ${reason}: ${sensors.length} sensors`);
            }
        }

        console.log(`\n  Total UNKNOWN: ${unknownSensors.length} sensors`);

        // Show sample sensors for each category
        console.log('\nSample UNKNOWN sensors by category:');
        for (const [category, sensors] of Object.entries(reasonCounts)) {
            if (sensors.length > 0) {
                console.log(`\n  ${category} (${sensors.length}):`);
                // Show up to 5 examples
                const samples = sensors.slice(0, 5);
                for (const { sensor, reasons } of samples) {
                    console.log(`    - ${sensor.node_name || sensor.device_id}`);
                    console.log(`      Reasons: ${reasons.join(', ')}`);
                }
                if (sensors.length > 5) {
                    console.log(`    ... and ${sensors.length - 5} more`);
                }
            }
        }
    }

    // Weather API failures
    if (weatherApiFailed.length > 0) {
        console.log('\n' + '-'.repeat(80));
        console.log('WEATHER API FAILURES (sensors not classified):');
        console.log('-'.repeat(80));
        console.log('These sensors could not be classified because weather data was unavailable.\n');

        // Group by failure reason
        const byReason = {};
        for (const r of weatherApiFailed) {
            const reason = r.weather_api_failure_reason || 'Unknown';
            if (!byReason[reason]) byReason[reason] = [];
            byReason[reason].push(r);
        }

        for (const [reason, sensors] of Object.entries(byReason)) {
            console.log(`  ${reason}: ${sensors.length} sensors`);
        }

        console.log('\nSensors with weather API failures:');
        for (const r of weatherApiFailed.slice(0, 10)) {
            const currentType = r.current_deployment_type || 'UNKNOWN';
            console.log(`  - ${r.node_name || r.device_id} (current: ${currentType})`);
        }
        if (weatherApiFailed.length > 10) {
            console.log(`  ... and ${weatherApiFailed.length - 10} more`);
        }
    }

    console.log('\n' + '='.repeat(80) + '\n');
}

/**
 * Import remote classification snapshots from the archive replicator.
 * Downloads _classifications/latest.json, compares confidence scores
 * against local classifications, and applies where remote is better.
 *
 * Merge rules per device:
 *   1. Remote has classification, local is blank/unknown → apply remote
 *   2. Both have classification, remote confidence > local → apply remote
 *   3. Both have classification, same confidence, remote is newer → apply remote
 *   4. Otherwise → keep local
 */
async function importRemoteClassifications() {
    if (!ARCHIVE_REPLICATOR_URL) return;

    const url = `${ARCHIVE_REPLICATOR_URL.replace(/\/$/, '')}/blobs/_classifications/latest.json`;

    try {
        const resp = await fetch(url, { signal: AbortSignal.timeout(10000) });
        if (!resp.ok) {
            console.log(`Remote classifications not available (HTTP ${resp.status})`);
            return;
        }

        const snapshot = await resp.json();
        if (!snapshot.classifications || !Array.isArray(snapshot.classifications)) {
            console.log('Remote classification snapshot has no classifications array');
            return;
        }

        const remote = snapshot.classifications.filter(
            c => c.device_id && c.deployment_type && c.deployment_type !== 'UNKNOWN'
        );
        if (remote.length === 0) {
            console.log(`Remote classification snapshot has ${snapshot.classifications.length} entries but none are usable (all UNKNOWN or missing fields)`);
            return;
        }

        console.log(`\nImporting remote classifications from ${snapshot.node_id || 'unknown'} (${remote.length} devices, exported ${snapshot.exported_at})`);

        // Load local classification state to compare confidence
        const stateFile = path.join(__dirname, '..', 'data', 'classification_state.json');
        let localState = {};
        try {
            const data = JSON.parse(fs.readFileSync(stateFile, 'utf-8'));
            localState = data.devices || {};
        } catch {
            // No local state — all remote classifications win
        }

        let applied = 0;
        let skippedLowerConfidence = 0;
        let skippedNoDevice = 0;
        const toApply = [];

        for (const rc of remote) {
            const local = localState[rc.device_id];
            const localConfidence = local?.confidence || 0;
            const localClassifiedAt = local?.classified_at || '';
            const localType = local?.last_result || '';

            // Merge decision
            let shouldApply = false;

            // Ignore remote classifications older than 30 days — sensor may have moved
            const maxAge = 30 * 24 * 60 * 60 * 1000;
            const remoteAge = Date.now() - new Date(rc.classified_at).getTime();
            if (remoteAge > maxAge) continue;

            if (!localType || localType === '' || localType === 'unknown' || localType === 'UNKNOWN') {
                // Local has no classification — apply remote
                shouldApply = true;
            } else if (rc.confidence > localConfidence) {
                // Remote has higher confidence — apply remote
                shouldApply = true;
            } else if (rc.confidence === localConfidence && rc.classified_at > localClassifiedAt) {
                // Same confidence, remote is newer — apply remote
                shouldApply = true;
            }

            if (shouldApply) {
                toApply.push({
                    device_id: rc.device_id,
                    inferred_deployment_type: rc.deployment_type,
                    deployment_confidence: rc.confidence,
                    node_name: rc.node_name || '',
                    latitude: rc.latitude || 0,
                    longitude: rc.longitude || 0,
                    weather_api_failed: false,
                    is_remote: true,
                });
            } else {
                skippedLowerConfidence++;
            }
        }

        if (toApply.length > 0) {
            console.log(`Applying ${toApply.length} remote classifications (skipped ${skippedLowerConfidence} with lower confidence)`);
            await applyClassifications(toApply, true); // overwrite=true since we already checked confidence
            applied = toApply.length;
        } else {
            console.log(`No remote classifications to apply (${skippedLowerConfidence} skipped — local confidence is higher or equal)`);
        }

        return applied;
    } catch (e) {
        console.log(`No remote classifications available: ${e.message}`);
        return 0;
    }
}

/**
 * Export classification snapshot to the archive replicator for P2P distribution.
 * Other guardians download this blob and merge it with their local classifications.
 * Merge rule: highest confidence wins, then most recent timestamp.
 *
 * Exports ALL known classifications from the state file, not just the current run.
 * This ensures new peers get the full picture, not just whatever was evaluated this cycle.
 */
async function exportClassificationSnapshot() {
    if (!ARCHIVE_REPLICATOR_URL) {
        console.log('ARCHIVE_REPLICATOR_URL not set — skipping P2P classification export');
        return;
    }

    // Read full classification state — this has every device ever classified
    const stateFile = path.join(__dirname, '..', 'data', 'classification_state.json');
    let stateData;
    try {
        stateData = JSON.parse(fs.readFileSync(stateFile, 'utf-8'));
    } catch {
        console.log('No classification state file — nothing to export');
        return;
    }

    const devices = stateData.devices || {};
    const classifiedIds = [];

    for (const [deviceId, entry] of Object.entries(devices)) {
        const type = entry.last_result;
        // Only export actual classifications, not failure states or UNKNOWN
        if (!type || type === 'UNKNOWN' || type === 'weather_api_failed' || type === 'insufficient_data' || type === 'skipped_source') {
            continue;
        }
        classifiedIds.push(deviceId);
    }

    if (classifiedIds.length === 0) {
        console.log('No classifications to export');
        return;
    }

    // Query ClickHouse for latest non-empty name and position of each classified device.
    // argMaxIf picks the most recent value where the field is non-empty/non-zero,
    // so a recent reading with blank name doesn't overwrite an older one that had it.
    const deviceList = classifiedIds.map(id => `'${id.replace(/'/g, "\\'")}'`).join(',');
    const metaQuery = `
        SELECT
            device_id,
            argMaxIf(node_name, timestamp, node_name != '') as node_name,
            argMaxIf(latitude, timestamp, latitude != 0) as latitude,
            argMaxIf(longitude, timestamp, longitude != 0) as longitude
        FROM wesense.sensor_readings
        WHERE device_id IN (${deviceList})
          AND timestamp > now() - INTERVAL 90 DAY
        GROUP BY device_id
    `;

    let deviceMeta = {};
    try {
        const result = await client.query({ query: metaQuery, format: 'JSONEachRow' });
        const rows = await result.json();
        for (const row of rows) {
            deviceMeta[row.device_id] = {
                node_name: row.node_name || '',
                latitude: parseFloat(row.latitude) || 0,
                longitude: parseFloat(row.longitude) || 0,
            };
        }
    } catch (e) {
        console.warn(`Failed to fetch device metadata for export: ${e.message}`);
        // Continue without metadata — classifications are still useful
    }

    const classifications = [];
    for (const deviceId of classifiedIds) {
        const entry = devices[deviceId];
        const meta = deviceMeta[deviceId] || {};
        classifications.push({
            device_id: deviceId,
            deployment_type: entry.last_result,
            confidence: entry.confidence || 0,
            classified_at: entry.classified_at || entry.last_attempt || '',
            node_name: meta.node_name || '',
            latitude: meta.latitude || 0,
            longitude: meta.longitude || 0,
        });
    }

    const snapshot = {
        exported_at: new Date().toISOString(),
        node_id: process.env.NODE_ID || 'unknown',
        classifications,
    };

    const blob = Buffer.from(JSON.stringify(snapshot));
    const url = `${ARCHIVE_REPLICATOR_URL.replace(/\/$/, '')}/blobs/_classifications/latest.json`;

    try {
        const resp = await fetch(url, {
            method: 'PUT',
            body: blob,
            headers: { 'Content-Type': 'application/octet-stream' },
            signal: AbortSignal.timeout(30000),
        });

        if (resp.ok) {
            const result = await resp.json();
            console.log(`Classification snapshot exported to archive replicator (${snapshot.classifications.length} devices, hash=${result.hash?.slice(0, 16)})`);
        } else {
            console.warn(`Failed to export classification snapshot: HTTP ${resp.status}`);
        }
    } catch (e) {
        console.warn(`Failed to export classification snapshot: ${e.message}`);
    }
}

/**
 * Apply classifications to ClickHouse database
 * Only updates Meshtastic sensors
 * @param {Array} results - Classification results
 * @param {boolean} overwrite - If true, overwrite ALL rows; if false, only update blank rows
 */
async function applyClassifications(results, overwrite = false) {
    console.log('\n' + '='.repeat(80));
    console.log('APPLYING CLASSIFICATIONS TO DATABASE');
    if (overwrite) {
        console.log('MODE: OVERWRITE (updating ALL rows)');
    } else {
        console.log('MODE: Normal (only updating blank deployment_type)');
    }
    console.log('='.repeat(80) + '\n');

    // Filter to sensors with a device_id and classification
    // Skip sensors where weather API failed - we can't classify without weather data
    // UNKNOWN sensors (with weather data) will have their deployment_type set to blank for future reclassification
    const weatherApiFailed = results.filter(r => r.weather_api_failed);
    if (weatherApiFailed.length > 0) {
        console.log(`Skipping ${weatherApiFailed.length} sensors where weather API failed:\n`);
        for (const r of weatherApiFailed.slice(0, 10)) {
            console.log(`  - ${r.node_name || r.device_id}: ${r.weather_api_failure_reason}`);
        }
        if (weatherApiFailed.length > 10) {
            console.log(`  ... and ${weatherApiFailed.length - 10} more\n`);
        }
        console.log('');
    }

    const toApply = results.filter(r =>
        r.device_id &&
        r.inferred_deployment_type &&
        !r.weather_api_failed  // Don't apply if weather API failed
    );

    const total = toApply.length;
    console.log(`Found ${total} sensors with valid classifications to apply\n`);

    let updated = 0;
    let skipped = 0;
    let errors = 0;

    for (let i = 0; i < toApply.length; i++) {
        const result = toApply[i];
        const deviceId = result.device_id;
        // UNKNOWN sensors get blank deployment_type so they're eligible for future reclassification
        const deploymentType = result.inferred_deployment_type === 'UNKNOWN' ? '' : result.inferred_deployment_type;
        const displayType = result.inferred_deployment_type; // For display purposes
        const progress = `[${i + 1}/${total}]`;
        const name = (result.node_name || deviceId).padEnd(30).slice(0, 30);

        try {
            // Check if this is a Meshtastic device (skip WeSense and others)
            const sourceQuery = `
                SELECT data_source, count() as total_rows FROM wesense.sensor_readings
                WHERE device_id = {deviceId:String}
                GROUP BY data_source
            `;
            const sourceResult = await client.query({
                query: sourceQuery,
                query_params: { deviceId },
                format: 'JSONEachRow'
            });
            const sourceRows = await sourceResult.json();
            const dataSource = (sourceRows[0]?.data_source || '').toUpperCase();
            const totalRows = sourceRows[0]?.total_rows || 0;

            if (!dataSource.startsWith('MESHTASTIC') && dataSource !== 'HOMEASSISTANT') {
                console.log(`${progress} ${name} → skipped (${dataSource || 'unknown source'}, ${totalRows} rows)`);
                skipped++;
                continue;
            }

            // Count rows that will be updated
            const countQuery = overwrite
                ? `SELECT count() as cnt FROM wesense.sensor_readings WHERE device_id = {deviceId:String}`
                : `SELECT count() as cnt FROM wesense.sensor_readings WHERE device_id = {deviceId:String} AND (deployment_type = '' OR deployment_type IS NULL)`;

            const countResult = await client.query({
                query: countQuery,
                query_params: { deviceId },
                format: 'JSONEachRow'
            });
            const countRows = await countResult.json();
            const rowCount = countRows[0]?.cnt || 0;

            if (rowCount === 0) {
                console.log(`${progress} ${name} → ${displayType} (0 rows to update, ${totalRows} total)`);
                skipped++;
                continue;
            }

            // Update rows - either all (overwrite) or just blank ones
            const query = overwrite
                ? `ALTER TABLE wesense.sensor_readings UPDATE deployment_type = {deploymentType:String} WHERE device_id = {deviceId:String}`
                : `ALTER TABLE wesense.sensor_readings UPDATE deployment_type = {deploymentType:String} WHERE device_id = {deviceId:String} AND (deployment_type = '' OR deployment_type IS NULL)`;

            await client.command({
                query,
                query_params: { deviceId, deploymentType }
            });

            // For remote imports: also fill in node_name and position on rows where they're blank
            if (result.is_remote) {
                if (result.node_name) {
                    await client.command({
                        query: `ALTER TABLE wesense.sensor_readings UPDATE node_name = {nodeName:String} WHERE device_id = {deviceId:String} AND (node_name = '' OR node_name IS NULL)`,
                        query_params: { deviceId, nodeName: result.node_name }
                    });
                }
                if (result.latitude && result.longitude) {
                    await client.command({
                        query: `ALTER TABLE wesense.sensor_readings UPDATE latitude = {lat:Float64}, longitude = {lng:Float64} WHERE device_id = {deviceId:String} AND (latitude = 0 OR longitude = 0)`,
                        query_params: { deviceId, lat: result.latitude, lng: result.longitude }
                    });
                }
            }

            const clearedNote = displayType === 'UNKNOWN' ? ' [cleared]' : '';
            console.log(`${progress} ${name} → ${displayType}${clearedNote} (${rowCount}/${totalRows} rows updated)`);
            updated++;

        } catch (error) {
            console.error(`${progress} ${name} ✗ ERROR: ${error.message}`);
            errors++;
        }

        // Small delay to avoid overwhelming ClickHouse
        await new Promise(resolve => setTimeout(resolve, 100));
    }

    console.log('\n' + '-'.repeat(80));
    console.log(`Applied: ${updated} | Skipped: ${skipped} | Errors: ${errors}`);
    console.log('-'.repeat(80) + '\n');

    return { updated, skipped, errors };
}

/**
 * Run the classifier (used by both manual and scheduled modes)
 * @param {number} days - Days of weather data to use
 * @param {boolean} shouldApply - Whether to apply classifications to database
 * @param {boolean} overwrite - Whether to overwrite existing classifications
 * @param {boolean} useSmartScheduling - Whether to use state-based filtering (default true in scheduler mode)
 */
async function runClassifier(days = 7, shouldApply = false, overwrite = false, useSmartScheduling = SCHEDULER_MODE) {
    console.log('Sensor Deployment Classifier');
    console.log(`Using 90 days history for variance/mobility, ${days} days for weather correlation\n`);

    // Load classification state for smart scheduling
    const state = new ClassificationState();
    if (useSmartScheduling) {
        state.load();
        const stateStats = state.getStats();
        if (stateStats.total_tracked > 0) {
            console.log(`Classification state: ${stateStats.total_tracked} tracked devices (${stateStats.eligible_now} eligible, ${stateStats.backing_off} backing off)`);
        }
    }

    // Get sensors (filtered to Meshtastic/HomeAssistant sensors with enough data for classification)
    // WeSense sensors are excluded - they have calibrated sensors and don't need classification
    console.log('Fetching sensors from ClickHouse (minimum 24 temperature readings required)...');

    const sensors = await getSensors();
    console.log(`Found ${sensors.length} sensors with sufficient data for classification\n`);

    if (sensors.length === 0) {
        console.log('No sensors found with location data.');
        return null;
    }

    // Smart scheduling: filter sensors through state
    let sensorsToEvaluate = sensors;
    const skippedSensors = [];

    if (useSmartScheduling) {
        sensorsToEvaluate = [];
        const skipReasons = {};
        const evalReasons = {};

        for (const sensor of sensors) {
            const rowCount = parseInt(sensor.reading_count) || 0;
            const { evaluate, reason } = state.shouldEvaluate(sensor.device_id, rowCount);

            if (evaluate) {
                sensorsToEvaluate.push(sensor);
                evalReasons[reason] = (evalReasons[reason] || 0) + 1;
            } else {
                skippedSensors.push(sensor);
                // Aggregate skip reasons for summary
                const key = reason.split(',')[0]; // e.g. "insufficient_data"
                skipReasons[key] = (skipReasons[key] || 0) + 1;
            }
        }

        console.log(`Smart scheduling: evaluating ${sensorsToEvaluate.length} of ${sensors.length} sensors`);
        if (sensorsToEvaluate.length > 0) {
            console.log('  Evaluating:');
            for (const [reason, count] of Object.entries(evalReasons).sort((a, b) => b[1] - a[1])) {
                console.log(`    ${reason}: ${count}`);
            }
        }
        if (skippedSensors.length > 0) {
            console.log(`  Skipping ${skippedSensors.length} (backoff):`);
            for (const [reason, count] of Object.entries(skipReasons).sort((a, b) => b[1] - a[1])) {
                console.log(`    ${reason}: ${count}`);
            }
        }
        console.log('');
    }

    if (sensorsToEvaluate.length === 0) {
        console.log('No sensors eligible for evaluation this run (all in backoff).');
        if (useSmartScheduling) {
            // Prune removed devices
            const activeIds = new Set(sensors.map(s => s.device_id));
            const pruned = state.prune(activeIds);
            if (pruned > 0) console.log(`Pruned ${pruned} removed devices from state`);
            state.save();
        }
        // Still export — we have historical classifications even if nothing was evaluated this cycle
        await exportClassificationSnapshot();
        return null;
    }

    // Classify eligible sensors
    const results = await classifyAllSensors(sensorsToEvaluate, days);

    // Print report
    printReport(results);

    // Save report to file
    saveReport(results);

    // Apply to database if requested
    if (shouldApply) {
        await applyClassifications(results, overwrite);
    }

    // Export classification snapshot for P2P distribution
    await exportClassificationSnapshot();

    // Record results in state and save
    if (useSmartScheduling) {
        for (const result of results) {
            const sensor = sensorsToEvaluate.find(s => s.device_id === result.device_id);
            const rowCount = sensor ? parseInt(sensor.reading_count) || 0 : 0;

            if (result.weather_api_failed) {
                state.recordResult(result.device_id, 'weather_api_failed', rowCount);
            } else if (result.inferred_deployment_type === 'UNKNOWN' && result.deployment_confidence === 0) {
                state.recordResult(result.device_id, 'insufficient_data', rowCount);
            } else {
                state.recordResult(result.device_id, 'classified', rowCount, result.inferred_deployment_type, result.deployment_confidence || 0);
            }
        }

        // Prune devices no longer in ClickHouse
        const activeIds = new Set(sensors.map(s => s.device_id));
        const pruned = state.prune(activeIds);
        if (pruned > 0) console.log(`Pruned ${pruned} removed devices from state`);

        state.save();

        // Log summary of what next run will look like
        const postStats = state.getStats();
        console.log('\nSmart scheduling summary:');
        console.log(`  Devices tracked: ${postStats.total_tracked}`);
        console.log(`  Will evaluate next run: ${postStats.eligible_now} (new/expired backoff)`);
        console.log(`  Will skip next run: ${postStats.backing_off} (in backoff)`);
        if (Object.keys(postStats.by_last_result).length > 0) {
            console.log('  By last result:');
            for (const [result, count] of Object.entries(postStats.by_last_result).sort((a, b) => b[1] - a[1])) {
                console.log(`    ${result}: ${count}`);
            }
        }
        if (Object.keys(postStats.by_skip_reason).length > 0) {
            console.log('  Backing off:');
            for (const [reason, count] of Object.entries(postStats.by_skip_reason).sort((a, b) => b[1] - a[1])) {
                console.log(`    ${reason}: ${count}`);
            }
        }
    }

    return results;
}

/**
 * Start scheduler mode - runs classifier on a cron schedule
 */
async function startScheduler() {
    // Dynamic import for node-cron (only needed in scheduler mode)
    const cron = await import('node-cron');

    const days = parseInt(process.env.CLASSIFIER_DAYS || '7');
    const shouldApply = !DRY_RUN;

    console.log('='.repeat(80));
    console.log('Sensor Deployment Classifier - SCHEDULER MODE');
    console.log('='.repeat(80));
    console.log(`Schedule: ${SCHEDULE}`);
    console.log(`Weather correlation days: ${days}`);
    console.log(`Dry run mode: ${DRY_RUN} (apply to database: ${shouldApply})`);
    console.log(`Run on startup: ${RUN_ON_STARTUP}`);
    console.log(`Started at: ${new Date().toISOString()}`);
    console.log('='.repeat(80) + '\n');

    // On startup: export existing classifications immediately (fast — just reads state file + CH metadata)
    // and import from peers. This ensures data flows before the slow classification cycle runs.
    console.log('Exporting existing classifications on startup...\n');
    try {
        await exportClassificationSnapshot();
    } catch (error) {
        console.error('Startup export failed:', error);
    }

    console.log('Importing remote classifications on startup...\n');
    try {
        await importRemoteClassifications();
    } catch (error) {
        console.error('Startup import failed:', error);
    }

    // Optionally run full classification immediately on startup
    if (RUN_ON_STARTUP) {
        console.log('Running initial classification...\n');
        try {
            await runClassifier(days, shouldApply);
        } catch (error) {
            console.error('Initial classification failed:', error);
        }
    } else {
        console.log('Waiting for scheduled run (use RUN_ON_STARTUP=true to run immediately)\n');
    }

    // Re-import after 5 minutes to catch peers that were still starting up
    setTimeout(async () => {
        console.log(`\n[${new Date().toISOString()}] Delayed import (5 min after startup)...\n`);
        try {
            await importRemoteClassifications();
        } catch (error) {
            console.error('Delayed import failed:', error);
        }
    }, 5 * 60 * 1000);

    // Schedule future runs: import from peers, then classify, then export
    cron.default.schedule(SCHEDULE, async () => {
        console.log(`\n[${new Date().toISOString()}] Scheduled run starting...\n`);
        try {
            await importRemoteClassifications();
            await runClassifier(days, shouldApply);
            console.log(`[${new Date().toISOString()}] Scheduled run complete.\n`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] Scheduled run failed:`, error);
        }
    });

    console.log('Scheduler running. Press Ctrl+C to stop.\n');

    // Keep process alive
    process.on('SIGINT', () => {
        console.log('\nShutting down scheduler...');
        process.exit(0);
    });
}

/**
 * Main entry point
 */
async function main() {
    // Check for scheduler mode first
    if (SCHEDULER_MODE) {
        await startScheduler();
        return;
    }

    const args = process.argv.slice(2);
    const showHelp = args.includes('--help') || args.includes('-h');
    const shouldApply = args.includes('--apply');
    const shouldOverwrite = args.includes('--overwrite');
    const applyFromFile = args.find(a => a.startsWith('--apply-from='))?.split('=')[1];
    const singleDevice = args.find(a => a.startsWith('--device='))?.split('=')[1];
    const days = parseInt(args.find(a => a.startsWith('--days='))?.split('=')[1] || '7');

    if (showHelp) {
        console.log(`
Sensor Deployment Classifier
============================

Classifies Meshtastic and HomeAssistant sensors as INDOOR, OUTDOOR, DEVICE, NON_AIR, MOBILE, or PORTABLE.
(WeSense sensors are excluded - they have calibrated sensors and don't need classification)

Classification strategies:
  - Temperature variance analysis (90 days history)
  - Weather correlation (using Open-Meteo API)
  - Temperature range heuristics
  - Device temperature detection

Usage:
  ./start.sh                      Run classification report only
  ./start.sh --apply              Run classification and apply to database
  ./start.sh --apply --overwrite  Apply to ALL rows (overwrite existing classifications)
  ./start.sh --apply-from=FILE    Apply from existing JSON report (skips classification)
  ./start.sh --device=ID          Classify a single device
  ./start.sh --days=14            Use 14 days for weather correlation (default: 7)
  ./start.sh --json               Output results as JSON
  ./start.sh --help               Show this help message

Scheduler Mode (Docker):
  Set CLASSIFIER_MODE=scheduler to run as a daemon with automatic scheduling.

  Environment variables:
    CLASSIFIER_MODE=scheduler   Enable scheduler mode
    CLASSIFIER_SCHEDULE         Cron schedule (default: "0 */12 * * *" = every 12 hours)
    CLASSIFIER_DAYS             Days for weather correlation (default: 7)
    DRY_RUN=true                Report only, don't write to database (default: false)
    RUN_ON_STARTUP=true         Run immediately when container starts (default: false)
    CLICKHOUSE_HOST             ClickHouse URL
    CLICKHOUSE_USER             ClickHouse username
    CLICKHOUSE_PASSWORD         ClickHouse password
    LOG_MAX_SIZE_KB             Max log file size before rotation (default: 10240)
    LOG_MAX_FILES               Number of rotated log files to keep (default: 5)

Thresholds (tunable in classifier.js):
  Indoor max variance: ${THRESHOLDS.INDOOR_MAX_VARIANCE}
  Outdoor min variance: ${THRESHOLDS.OUTDOOR_MIN_VARIANCE}
  Outdoor min weather correlation: ${THRESHOLDS.OUTDOOR_MIN_CORRELATION}
  Indoor max weather correlation: ${THRESHOLDS.INDOOR_MAX_CORRELATION}
`);
        return;
    }

    // Apply from existing file (skip classification)
    if (applyFromFile) {
        console.log('Sensor Deployment Classifier');
        console.log(`Applying classifications from: ${applyFromFile}\n`);

        if (!fs.existsSync(applyFromFile)) {
            console.error(`File not found: ${applyFromFile}`);
            process.exit(1);
        }

        try {
            const reportData = JSON.parse(fs.readFileSync(applyFromFile, 'utf-8'));
            const results = reportData.sensors || reportData;

            if (!Array.isArray(results)) {
                console.error('Invalid report format: expected "sensors" array');
                process.exit(1);
            }

            console.log(`Loaded ${results.length} sensor classifications\n`);
            await applyClassifications(results, shouldOverwrite);
        } catch (error) {
            console.error(`Failed to read report: ${error.message}`);
            process.exit(1);
        }
        return;
    }

    if (singleDevice) {
        // Single device mode
        console.log('Sensor Deployment Classifier');
        console.log(`Using 90 days history for variance/mobility, ${days} days for weather correlation\n`);

        console.log('Fetching sensors from ClickHouse...');
        const sensors = await getSensors();

        const sensor = sensors.find(s => s.device_id === singleDevice);
        if (!sensor) {
            console.log(`Device ${singleDevice} not found (only Meshtastic/HomeAssistant sensors are classified)`);
            return;
        }

        console.log(`Classifying single device: ${singleDevice}\n`);
        const result = await classifySensor(sensor, days);
        const results = [result];

        printReport(results);
        saveReport(results);

        if (shouldApply) {
            await applyClassifications(results, shouldOverwrite);
        }
    } else {
        // Full classification
        await runClassifier(days, shouldApply, shouldOverwrite);
    }

    // Output JSON for further processing
    if (args.includes('--json')) {
        console.log('\nJSON Output:');
        const sensors = await getSensors();
        const results = await classifyAllSensors(sensors, days);
        console.log(JSON.stringify(results, null, 2));
    }
}

main().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});
