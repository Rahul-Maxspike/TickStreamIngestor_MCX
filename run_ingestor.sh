#!/bin/bash
set -euo pipefail

# TickStreamIngestor - Cron-compatible startup script
# Starts the ingestor in a new terminal with proper environment isolation

REPO_DIR="/home/algolinux/Documents/aviral/Github/TickStreamIngestor_MCX"
LOG_DIR="$REPO_DIR/logs"
PID_FILE="$REPO_DIR/ingestor.pid"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/ingestor_${TIMESTAMP}.log"

echo "=========================================="
echo "TickStreamIngestor Startup"
echo "=========================================="

# Create log directory
mkdir -p "$LOG_DIR"

# Check if already running
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "$(date): ⚠️  Ingestor already running (PID $OLD_PID)" | tee -a "$LOG_FILE"
        echo "   Skipping startup to prevent duplicate processes."
        exit 0
    else
        echo "$(date): Removing stale PID file" | tee -a "$LOG_FILE"
        rm "$PID_FILE"
    fi
fi

# Verify .env file exists
if [ ! -f "$REPO_DIR/.env" ]; then
    echo "❌ Error: .env file not found in $REPO_DIR" | tee -a "$LOG_FILE"
    echo "   Please create .env with Redis/ClickHouse configuration"
    exit 1
fi

# Verify Python script exists
if [ ! -f "$REPO_DIR/run_ingestor.py" ]; then
    echo "❌ Error: run_ingestor.py not found in $REPO_DIR" | tee -a "$LOG_FILE"
    exit 1
fi

# ---- helper: launch terminal with clean environment (cron-safe) ----
launch_terminal_clean() {
    local title="$1"
    local cmd="$2"
    local uid user_name home_dir display xauth dbus runtime_dir

    uid="$(id -u)"
    user_name="$(id -un)"

    # Home directory
    home_dir="${HOME:-$(getent passwd "$user_name" | cut -d: -f6)}"

    # X Display (adjust if needed - check with: ls /tmp/.X11-unix/)
    display="${DISPLAY:-:0}"

    # Xauthority for GUI access
    xauth="${XAUTHORITY:-$home_dir/.Xauthority}"
    if [ ! -f "$xauth" ] && [ -f "/run/user/$uid/gdm/Xauthority" ]; then
        xauth="/run/user/$uid/gdm/Xauthority"
    fi

    # DBus session (required for gnome-terminal from cron)
    dbus="${DBUS_SESSION_BUS_ADDRESS:-unix:path=/run/user/$uid/bus}"

    # XDG runtime directory
    runtime_dir="${XDG_RUNTIME_DIR:-/run/user/$uid}"

    # Launch with minimal clean environment
    env -i \
        HOME="$home_dir" \
        USER="$user_name" \
        LOGNAME="${LOGNAME:-$user_name}" \
        SHELL="/bin/bash" \
        PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
        DISPLAY="$display" \
        XAUTHORITY="$xauth" \
        DBUS_SESSION_BUS_ADDRESS="$dbus" \
        XDG_RUNTIME_DIR="$runtime_dir" \
        /usr/bin/gnome-terminal --title="$title" -- bash -lc "$cmd"
}

echo "Starting TickStreamIngestor..."
echo "  Working directory: $REPO_DIR"
echo "  Log file: $LOG_FILE"

# Launch ingestor in new terminal
launch_terminal_clean \
    "TickStreamIngestor - Redis → ClickHouse" \
    "cd \"$REPO_DIR\"; python3 run_ingestor.py 2>&1 | tee \"$LOG_FILE\"; echo 'Press Enter to close...'; read" &

# Save PID of the launch process
LAUNCH_PID=$!
sleep 2

# Find the actual Python process PID
PYTHON_PID=$(pgrep -f "python3 run_ingestor.py" | head -1)

if [ -n "$PYTHON_PID" ]; then
    echo "$PYTHON_PID" > "$PID_FILE"
    echo "$(date): ✅ Ingestor started successfully (PID $PYTHON_PID)" | tee -a "$LOG_FILE"
else
    echo "$(date): ⚠️  Could not find Python process PID" | tee -a "$LOG_FILE"
    echo "   Terminal launched but process detection failed"
fi

echo "=========================================="
echo "✅ TickStreamIngestor startup complete"
echo "=========================================="
echo ""
echo "Monitor logs: tail -f $LOG_FILE"
echo "Check status:  ps -p \$(cat $PID_FILE)"
echo "Stop ingestor: $REPO_DIR/stop_ingestor.sh"
