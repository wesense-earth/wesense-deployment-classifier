FROM node:20-slim

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install --omit=dev

# Copy source code
COPY src/ ./src/

# Copy entrypoint script
COPY entrypoint.sh ./
RUN chmod +x entrypoint.sh

# Create directories for reports and logs
RUN mkdir -p /app/reports /app/logs /app/data

# Environment variables (can be overridden via docker run -e)
# Note: Configure these via docker run -e or use docker_run.sh with .env file
ENV CLICKHOUSE_HOST=http://localhost:8123
ENV CLICKHOUSE_USER=wesense
ENV CLICKHOUSE_PASSWORD=
ENV CLICKHOUSE_DATABASE=wesense

# Scheduler settings
ENV CLASSIFIER_MODE=scheduler
ENV CLASSIFIER_SCHEDULE="0 */12 * * *"
ENV CLASSIFIER_DAYS=7

# Dry run mode - set to true to skip database updates
ENV DRY_RUN=false

# Run on startup - set to true to run classification immediately when container starts
ENV RUN_ON_STARTUP=false

# Log rotation settings
ENV LOG_MAX_SIZE_KB=10240
ENV LOG_MAX_FILES=5

# Run via entrypoint for logging support
CMD ["./entrypoint.sh"]
