#!/usr/bin/env bash
# =============================================================================
# collect-logs.sh — Copy logs from local /tmp to the network mount on each node
#
# Since all cluster nodes already have the home directory mounted locally,
# this script simply SSHes into each host and does a local cp from /tmp
# to the mount point — no rsync or network transfer needed.
#
# Run this after the experiment is finished and processes have been shut down.
#
# Usage:  ./collect-logs.sh [net-map-file]
#         Default net-map file: net-map.txt in the same directory as this script
# =============================================================================

set -uo pipefail

# -----------------------------------------------------------------------------
# REQUIRED — you MUST set these before running (must match launch-to-tmp.sh)
# -----------------------------------------------------------------------------

# Your group's home directory on the cluster (must match launch-to-tmp.sh)
# Replace XX with your group number, e.g. /home/distalg03
SHARED_DIR="/home/distalg06"

# -----------------------------------------------------------------------------
# OPTIONAL — safe to leave as-is, adjust only if needed
# -----------------------------------------------------------------------------

# Local directory on each remote machine where logs were written (must match launch-to-tmp.sh)
LOG_BASE_DIR_LOCAL="/tmp/distalg-logs"

# Network mount path where logs will be copied to (inside your shared home directory)
LOG_BASE_DIR_NETWORK="${SHARED_DIR}/logs"

# If true, delete the local /tmp log directory on each node after a successful copy.
# Set to false while debugging to keep local logs intact if something goes wrong.
REMOVE_AFTER_COPY=true

# SSH user on the remote machines (leave empty to use your current user)
SSH_USER=""

# Extra SSH options
SSH_OPTS="-n -o StrictHostKeyChecking=no -o BatchMode=yes"

# -----------------------------------------------------------------------------
# END OF CONFIGURATION
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NET_MAP="${1:-${SCRIPT_DIR}/net-map.txt}"

if [[ ! -f "$NET_MAP" ]]; then
    echo "ERROR: net-map file not found: $NET_MAP" >&2
    exit 1
fi

# Collect the unique set of hosts from the net-map (order preserved)
mapfile -t HOSTS < <(awk '{print $1}' "$NET_MAP" | awk '!seen[$0]++')

total=${#HOSTS[@]}
count=0

echo "==> Collecting logs from cluster nodes"
echo "==> Net-map:            $NET_MAP"
echo "==> Unique hosts:       $total"
echo "==> Source (local):     $LOG_BASE_DIR_LOCAL"
echo "==> Destination:        $LOG_BASE_DIR_NETWORK  (network mount on each node)"
echo "==> Remove after copy:  $REMOVE_AFTER_COPY"
echo ""

for host in "${HOSTS[@]}"; do
    count=$((count + 1))

    if [[ -n "$SSH_USER" ]]; then
        SSH_TARGET="${SSH_USER}@${host}"
    else
        SSH_TARGET="${host}"
    fi

    echo "[${count}/${total}] ${host} — copying ${LOG_BASE_DIR_LOCAL}/ ..."

    # SSH into the node and copy locally from /tmp to the network mount.
    # cp -r copies the contents of the local log dir into the destination.
    # mkdir -p ensures the destination exists before copying.
    # The local logs are only removed if REMOVE_AFTER_COPY=true AND the copy succeeded.
    if [[ "$REMOVE_AFTER_COPY" == "true" ]]; then
        REMOTE_CMD="mkdir -p '${LOG_BASE_DIR_NETWORK}' && \
cp -r '${LOG_BASE_DIR_LOCAL}'/. '${LOG_BASE_DIR_NETWORK}/' && \
rm -rf '${LOG_BASE_DIR_LOCAL}' && \
echo 'copy and cleanup done'"
    else
        REMOTE_CMD="mkdir -p '${LOG_BASE_DIR_NETWORK}' && \
cp -r '${LOG_BASE_DIR_LOCAL}'/. '${LOG_BASE_DIR_NETWORK}/' && \
echo 'copy done (local logs preserved)'"
    fi

    # shellcheck disable=SC2086
    if ssh ${SSH_OPTS} "$SSH_TARGET" "$REMOTE_CMD"; then
        echo "              ✓ done"
    else
        echo "              ✗ FAILED on ${host}" >&2
    fi

done

echo ""
echo "==> Log collection complete from ${count} hosts."
echo "==> Logs available at: ${LOG_BASE_DIR_NETWORK}"
