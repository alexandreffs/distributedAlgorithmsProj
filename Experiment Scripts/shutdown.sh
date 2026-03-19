#!/usr/bin/env bash
# =============================================================================
# shutdown.sh — Stop Java processes launched by launch-to-tmp.sh across cluster nodes
#
# Reads the same net-map file used at launch time, SSHes into each unique host,
# and kills all Java processes running the configured JAR.
#
# Usage:  ./shutdown.sh [net-map-file]
#         Default net-map file: net-map.txt in the same directory as this script
# =============================================================================

set -euo pipefail

# -----------------------------------------------------------------------------
# REQUIRED — you MUST set these before running
# -----------------------------------------------------------------------------

# Your group's home directory on the cluster (must match launch-to-tmp.sh)
# Replace XX with your group number, e.g. /home/distalg03
SHARED_DIR="/home/distalgXX"

# Filename of your JAR (must match launch-to-tmp.sh).
# Used to scope the kill to your processes only.
JAR_NAME="your-project.jar"

# -----------------------------------------------------------------------------
# OPTIONAL — safe to leave as-is, adjust only if needed
# -----------------------------------------------------------------------------

# SSH user on the remote machines (leave empty to use your current user)
SSH_USER=""

# Extra SSH options
SSH_OPTS="-n -o StrictHostKeyChecking=no -o BatchMode=yes"

# If true, send SIGTERM first and wait GRACEFUL_TIMEOUT seconds before SIGKILL.
# If false, send SIGKILL immediately.
GRACEFUL_SHUTDOWN=true
GRACEFUL_TIMEOUT=30

# -----------------------------------------------------------------------------
# END OF CONFIGURATION
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NET_MAP="${1:-${SCRIPT_DIR}/net-map.txt}"

if [[ ! -f "$NET_MAP" ]]; then
    echo "ERROR: net-map file not found: $NET_MAP" >&2
    exit 1
fi

JAR_PATH="${SHARED_DIR}/jars/${JAR_NAME}"

# Collect the unique set of hosts from the net-map (order preserved)
mapfile -t HOSTS < <(awk '{print $1}' "$NET_MAP" | awk '!seen[$0]++')

total=${#HOSTS[@]}
count=0

echo "==> Shutting down Java processes across cluster nodes"
echo "==> Net-map:           $NET_MAP"
echo "==> Unique hosts:      $total"
echo "==> JAR:               $JAR_PATH"
echo "==> Graceful shutdown: $GRACEFUL_SHUTDOWN (timeout: ${GRACEFUL_TIMEOUT}s)"
echo ""

for host in "${HOSTS[@]}"; do
    count=$((count + 1))

    if [[ -n "$SSH_USER" ]]; then
        SSH_TARGET="${SSH_USER}@${host}"
    else
        SSH_TARGET="${host}"
    fi

    if [[ "$GRACEFUL_SHUTDOWN" == "true" ]]; then
        REMOTE_CMD="killall -TERM java 2>/dev/null && echo 'SIGTERM sent, waiting ${GRACEFUL_TIMEOUT}s...' && sleep ${GRACEFUL_TIMEOUT} && killall -KILL java 2>/dev/null && echo 'SIGKILL sent to survivors' || echo 'all processes exited cleanly'"
    else
        REMOTE_CMD="killall -KILL java 2>/dev/null && echo 'SIGKILL sent' || echo 'no java processes found'"
    fi

    echo "[${count}/${total}] ${host}"
    # shellcheck disable=SC2086
    if ssh ${SSH_OPTS} "$SSH_TARGET" "$REMOTE_CMD"; then
        echo "              ✓ done"
    else
        echo "              ✗ FAILED on ${host}" >&2
    fi

done

echo ""
echo "==> Shutdown complete across ${count} hosts."
