import fs from 'fs';
import path from 'path';

const STATE_VERSION = 1;

// Backoff schedule: consecutive_skips → days until next retry
const BACKOFF_SCHEDULE = [1, 2, 4, 7, 14]; // index = consecutive_skips - 1, capped at last value

// Successfully classified sensors are re-evaluated after this many days
const SUCCESSFUL_REEVAL_DAYS = 7;

// If row count grew by this fraction since last attempt, reset backoff and re-evaluate
const ROW_GROWTH_THRESHOLD = 0.5; // 50%

/**
 * Classification state manager with exponential backoff.
 *
 * Tracks per-device attempt history in a JSON file so the scheduler
 * can skip sensors that don't need re-evaluation yet.
 */
export class ClassificationState {
    constructor(statePath) {
        this.statePath = statePath || '/app/data/classification_state.json';
        this.state = { version: STATE_VERSION, devices: {} };
    }

    /**
     * Load state from disk. Creates empty state if file missing or corrupt.
     */
    load() {
        try {
            if (fs.existsSync(this.statePath)) {
                const raw = fs.readFileSync(this.statePath, 'utf-8');
                const parsed = JSON.parse(raw);
                if (parsed.version === STATE_VERSION && parsed.devices) {
                    this.state = parsed;
                } else {
                    console.log(`Classification state version mismatch (got ${parsed.version}, expected ${STATE_VERSION}), starting fresh`);
                    this.state = { version: STATE_VERSION, devices: {} };
                }
            } else {
                console.log('No classification state file found, starting fresh');
            }
        } catch (error) {
            console.error(`Failed to load classification state: ${error.message}, starting fresh`);
            this.state = { version: STATE_VERSION, devices: {} };
        }
    }

    /**
     * Atomic save: write to .tmp then rename.
     */
    save() {
        const dir = path.dirname(this.statePath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }

        const tmpPath = this.statePath + '.tmp';
        fs.writeFileSync(tmpPath, JSON.stringify(this.state, null, 2));
        fs.renameSync(tmpPath, this.statePath);
    }

    /**
     * Determine whether a device should be evaluated this run.
     *
     * @param {string} deviceId
     * @param {number} currentRowCount - current reading count from ClickHouse
     * @returns {{ evaluate: boolean, reason: string }}
     */
    shouldEvaluate(deviceId, currentRowCount) {
        const entry = this.state.devices[deviceId];

        if (!entry) {
            return { evaluate: true, reason: 'new device' };
        }

        const now = Date.now();
        const nextEligible = new Date(entry.next_eligible).getTime();

        // Check for significant data growth (>50% more rows) — reset backoff
        if (entry.row_count && currentRowCount > 0) {
            const growth = (currentRowCount - entry.row_count) / entry.row_count;
            if (growth > ROW_GROWTH_THRESHOLD) {
                return { evaluate: true, reason: `data grew ${(growth * 100).toFixed(0)}% (${entry.row_count} → ${currentRowCount})` };
            }
        }

        // Check if past next_eligible time
        if (now >= nextEligible) {
            return { evaluate: true, reason: 'backoff expired' };
        }

        // Still in backoff
        const remainingMs = nextEligible - now;
        const remainingHours = Math.ceil(remainingMs / (1000 * 60 * 60));
        const remainingDays = (remainingMs / (1000 * 60 * 60 * 24)).toFixed(1);

        const skipReason = entry.skip_reason || entry.last_result;
        if (remainingHours < 24) {
            return { evaluate: false, reason: `${skipReason}, eligible in ${remainingHours}h` };
        }
        return { evaluate: false, reason: `${skipReason}, eligible in ${remainingDays}d` };
    }

    /**
     * Record the result of a classification attempt.
     *
     * @param {string} deviceId
     * @param {string} result - one of: "classified", "insufficient_data", "weather_api_failed", "skipped_source"
     * @param {number} rowCount - current reading count
     * @param {string|null} classification - the actual type (INDOOR, OUTDOOR, etc.) if result === "classified"
     */
    recordResult(deviceId, result, rowCount, classification = null) {
        const now = new Date();
        const existing = this.state.devices[deviceId] || {
            attempts: 0,
            consecutive_skips: 0
        };

        const entry = {
            last_attempt: now.toISOString(),
            last_result: result === 'classified' ? (classification || 'classified') : result,
            attempts: existing.attempts + 1,
            row_count: rowCount || 0,
            consecutive_skips: 0,
            skip_reason: null,
            next_eligible: null
        };

        if (result === 'classified') {
            // Successfully classified — re-evaluate in 7 days
            entry.consecutive_skips = 0;
            entry.skip_reason = null;
            entry.next_eligible = new Date(now.getTime() + SUCCESSFUL_REEVAL_DAYS * 24 * 60 * 60 * 1000).toISOString();
        } else {
            // Failed — apply exponential backoff
            entry.consecutive_skips = existing.consecutive_skips + 1;
            entry.skip_reason = result;
            const backoffIndex = Math.min(entry.consecutive_skips - 1, BACKOFF_SCHEDULE.length - 1);
            const backoffDays = BACKOFF_SCHEDULE[backoffIndex];
            entry.next_eligible = new Date(now.getTime() + backoffDays * 24 * 60 * 60 * 1000).toISOString();
        }

        this.state.devices[deviceId] = entry;
    }

    /**
     * Get summary statistics about the state.
     */
    getStats() {
        const devices = Object.values(this.state.devices);
        const now = Date.now();

        const stats = {
            total_tracked: devices.length,
            eligible_now: 0,
            backing_off: 0,
            by_skip_reason: {},
            by_last_result: {}
        };

        for (const d of devices) {
            // Count eligible vs backing off
            const nextEligible = new Date(d.next_eligible).getTime();
            if (now >= nextEligible) {
                stats.eligible_now++;
            } else {
                stats.backing_off++;
            }

            // Count by skip reason
            if (d.skip_reason) {
                stats.by_skip_reason[d.skip_reason] = (stats.by_skip_reason[d.skip_reason] || 0) + 1;
            }

            // Count by last result
            const result = d.last_result || 'unknown';
            stats.by_last_result[result] = (stats.by_last_result[result] || 0) + 1;
        }

        return stats;
    }

    /**
     * Remove devices no longer in ClickHouse to prevent state file bloat.
     *
     * @param {Set<string>} activeDeviceIds - device IDs currently in ClickHouse
     * @returns {number} number of pruned entries
     */
    prune(activeDeviceIds) {
        let pruned = 0;
        for (const deviceId of Object.keys(this.state.devices)) {
            if (!activeDeviceIds.has(deviceId)) {
                delete this.state.devices[deviceId];
                pruned++;
            }
        }
        return pruned;
    }
}
