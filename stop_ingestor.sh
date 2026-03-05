#!/bin/bash
set -euo pipefail

# TickStreamIngestor - Stop script

REPO_DIR="/home/quantlinux/Documents/TickStreamIngestor"
PID_FILE="$REPO_DIR/ingestor.pid"
LOG_DIR="$REPO_DIR/logs"

echo "=========================================="
echo "Stopping TickStreamIngestor"
echo "=========================================="

if [ ! -f "$PID_FILE" ]; then
    echo "⚠️  No PID file found ($PID_FILE)"
    echo "   Ingestor may not be running or was not started via run_ingestor.sh"
    exit 1
fi

PID=$(cat "$PID_FILE")

if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "⚠️  Process $PID not found (already stopped?)"
    echo "   Removing stale PID file"
    rm "$PID_FILE"
    exit 0
fi

echo "Stopping ingestor (PID $PID)..."
kill -TERM "$PID" 2>/dev/null || true

# Wait for graceful shutdown (max 30 seconds)
for i in {1..30}; do
    if ! ps -p "$PID" > /dev/null 2>&1; then
        echo "✅ Ingestor stopped gracefully"
        rm "$PID_FILE"
        echo "$(date): Ingestor stopped" >> "$LOG_DIR/ingestor_$(date +%Y%m%d).log"
        exit 0
    fi
    sleep 1
done

# Force kill if still running
echo "⚠️  Graceful shutdown timeout, forcing stop..."
kill -KILL "$PID" 2>/dev/null || true
rm "$PID_FILE"
echo "✅ Ingestor force stopped"
echo "$(date): Ingestor force killed" >> "$LOG_DIR/ingestor_$(date +%Y%m%d).log"
