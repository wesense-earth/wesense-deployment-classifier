#!/usr/bin/env node

import fs from 'fs';
import { getSensors } from './src/clickhouse-client.js';
import { classifySensor } from './src/classifier.js';

async function main() {
    // Read MIXED sensor device_ids from the apply file
    const applyData = JSON.parse(fs.readFileSync('reports/classification-2026-01-06T22-56-53-apply.json', 'utf-8'));
    const sensors = applyData.sensors || applyData;
    const mixedDeviceIds = new Set(
        sensors.filter(s => s.inferred_deployment_type === 'MIXED')
                .map(s => s.device_id)
    );
    
    console.log(`Found ${mixedDeviceIds.size} MIXED sensors to reclassify\n`);
    
    // Get all sensors from ClickHouse
    console.log('Fetching sensors from ClickHouse...');
    const allSensors = await getSensors();
    
    // Filter to just the MIXED ones
    const mixedSensors = allSensors.filter(s => mixedDeviceIds.has(s.device_id));
    console.log(`Matched ${mixedSensors.length} sensors in database\n`);
    
    // Reclassify each
    const results = [];
    const total = mixedSensors.length;
    
    for (let i = 0; i < mixedSensors.length; i++) {
        const sensor = mixedSensors[i];
        const result = await classifySensor(sensor, 7);
        results.push(result);
        
        const name = (sensor.node_name || sensor.device_id).padEnd(30).slice(0, 30);
        const deployment = result.inferred_deployment_type.padEnd(10);
        const confidence = `${(result.deployment_confidence * 100).toFixed(0)}%`;
        const rows = sensor.reading_count || 0;
        
        console.log(`[${i + 1}/${total}] ${name} → ${deployment} ${confidence} (${rows} rows)`);
        
        // Rate limit
        await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    // Summary
    const counts = {};
    for (const r of results) {
        counts[r.inferred_deployment_type] = (counts[r.inferred_deployment_type] || 0) + 1;
    }
    
    console.log('\n' + '-'.repeat(60));
    console.log('RECLASSIFICATION SUMMARY (previously MIXED):');
    console.log('-'.repeat(60));
    for (const [type, count] of Object.entries(counts).sort((a, b) => b[1] - a[1])) {
        console.log(`  ${type.padEnd(10)} ${count}`);
    }
    console.log('-'.repeat(60));
    
    // Write output file
    const output = {
        generated_at: new Date().toISOString(),
        note: 'Reclassified MIXED sensors with updated logic',
        sensors: results
    };
    
    fs.writeFileSync('reports/classification-mixed-reclassified.json', JSON.stringify(output, null, 2));
    console.log('\nWrote: reports/classification-mixed-reclassified.json');
    console.log('To apply: ./start.sh --apply-from=reports/classification-mixed-reclassified.json --overwrite');
}

main().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
