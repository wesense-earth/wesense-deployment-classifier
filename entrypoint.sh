#!/bin/sh
# Entrypoint script — fix ownership, drop privileges, rolling log support
set -e

PUID="${PUID:-1000}"
PGID="${PGID:-1000}"

LOG_DIR="/app/logs"
LOG_FILE="$LOG_DIR/classifier.log"
MAX_LOG_SIZE_KB="${LOG_MAX_SIZE_KB:-10240}"  # Default 10MB
MAX_LOG_FILES="${LOG_MAX_FILES:-5}"          # Keep 5 rotated files

# Ensure writable directories exist with correct ownership
mkdir -p "$LOG_DIR" /app/reports /app/data
chown -R "$PUID:$PGID" "$LOG_DIR" /app/reports /app/data

# Function to rotate logs
rotate_logs() {
    if [ -f "$LOG_FILE" ]; then
        size=$(du -k "$LOG_FILE" 2>/dev/null | cut -f1)
        if [ "$size" -ge "$MAX_LOG_SIZE_KB" ]; then
            echo "[$(date -Iseconds)] Rotating logs (size: ${size}KB >= ${MAX_LOG_SIZE_KB}KB)" >> "$LOG_FILE"

            # Remove oldest log file
            [ -f "$LOG_FILE.$MAX_LOG_FILES" ] && rm "$LOG_FILE.$MAX_LOG_FILES"

            # Rotate existing logs
            i=$((MAX_LOG_FILES - 1))
            while [ $i -ge 1 ]; do
                [ -f "$LOG_FILE.$i" ] && mv "$LOG_FILE.$i" "$LOG_FILE.$((i + 1))"
                i=$((i - 1))
            done

            # Rotate current log
            mv "$LOG_FILE" "$LOG_FILE.1"
            touch "$LOG_FILE"
        fi
    fi
}

# Print startup info
echo "=============================================="
echo "WeSense Deployment Classifier"
echo "=============================================="
echo "Started at: $(date -Iseconds)"
echo "Schedule: ${CLASSIFIER_SCHEDULE:-0 */12 * * *}"
echo "Days for weather correlation: ${CLASSIFIER_DAYS:-7}"
echo "Dry run mode: ${DRY_RUN:-false}"
echo "Run on startup: ${RUN_ON_STARTUP:-false}"
echo "Log file: $LOG_FILE"
echo "Log rotation: ${MAX_LOG_SIZE_KB}KB max, keep $MAX_LOG_FILES files"
echo "=============================================="

# If DRY_RUN is set, don't apply changes
if [ "$DRY_RUN" = "true" ] || [ "$DRY_RUN" = "1" ]; then
    export CLASSIFIER_DRY_RUN=true
fi

# Start log rotation check in background (every 5 minutes)
while true; do
    sleep 300
    rotate_logs
done &

# Drop privileges and run the classifier with output to both console and log file
exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups \
    sh -c 'node src/index.js 2>&1 | tee -a "'"$LOG_FILE"'"'
