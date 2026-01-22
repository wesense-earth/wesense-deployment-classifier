#!/bin/bash

# WeSense Deployment Classifier - Docker Run Script
# Runs the classifier as a scheduled daemon (3am daily by default)
#
# Configure your credentials in .env file before running this script.
# See .env.sample for the required variables.

# Load .env file if it exists
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

docker run -d \
  --name wesense-deployment-classifier \
  --restart unless-stopped \
  -e CLASSIFIER_MODE=scheduler \
  -e CLASSIFIER_SCHEDULE="${CLASSIFIER_SCHEDULE:-0 3 * * *}" \
  -e CLASSIFIER_DAYS="${CLASSIFIER_DAYS:-7}" \
  -e CLICKHOUSE_HOST="${CLICKHOUSE_HOST}" \
  -e CLICKHOUSE_USER="${CLICKHOUSE_USER}" \
  -e CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}" \
  -e CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-wesense}" \
  -v "$(pwd)/reports:/app/reports" \
  wesense-deployment-classifier
