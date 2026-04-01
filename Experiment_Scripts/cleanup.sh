#!/usr/bin/env bash
# =============================================================================
# cleanup.sh — Remove experiment temp files on all nodes
#
# For each unique host in the net-map this script will:
#   1. Delete the local /tmp log directory used during the experiment
#   2. Optionally run: docker system prune --all --force  (disabled by default)
#
# Usage:  ./cleanup.sh [net-map-file]
#         Default net-map file: net-map.txt in the same directory as this script
# =============================================================================

set -uo pipefail

# -----------------------------------------------------------------------------
# REQUIRED — you MUST set these before running (must match launch-to-tmp.sh)
# -----------------------------------------------------------------------------

# Your group's home directory on the cluster (must match launch-to-tmp.sh)
# Replace XX with your group number, e.g. /home/distalg03
SHARED_DIR="/home/distalgXX"

# -----------------------------------------------------------------------------
# OPTIONAL — safe to leave as-is, adjust only if needed
# -----------------------------------------------------------------------------

# Local log directory to remove on each remote machine (must match launch-to-tmp.sh)
LOG_BASE_DIR_LOCAL="/tmp/distalg-logs"

# Set to true to also run "docker system prune --all --force" on each node.
# This removes all unused Docker images, containers, and volumes.
# Only needed if your experiment uses Docker. Leave false otherwise.
DOCKER_PRUNE=false

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

# Collect unique hosts, preserving order
mapfile -t HOSTS < <(awk '{print $1}' "$NET_MAP" | awk '!seen[$0]++')

total=${#HOSTS[@]}
count=0

echo "==> Cleaning up cluster nodes"
echo "==> Net-map:      $NET_MAP"
echo "==> Unique hosts: $total"
echo "==> Will remove:  $LOG_BASE_DIR_LOCAL"
echo "==> Docker prune: $DOCKER_PRUNE"
echo ""

for host in "${HOSTS[@]}"; do
    count=$((count + 1))

    if [[ -n "$SSH_USER" ]]; then
        SSH_TARGET="${SSH_USER}@${host}"
    else
        SSH_TARGET="${host}"
    fi

    echo "[${count}/${total}] ${host}"

    # Remove only the specific log directory.
    # The rm -rf is scoped to the exact path so everything else in /tmp
    # is left completely untouched.
    if [[ "$DOCKER_PRUNE" == "true" ]]; then
        REMOTE_CMD="rm -rf '${LOG_BASE_DIR_LOCAL}' && docker system prune --all --force"
    else
        REMOTE_CMD="rm -rf '${LOG_BASE_DIR_LOCAL}'"
    fi

    # shellcheck disable=SC2086
    if ssh ${SSH_OPTS} "$SSH_TARGET" "$REMOTE_CMD"; then
        echo "              ✓ done"
    else
        echo "              ✗ FAILED on ${host}" >&2
    fi

done

echo ""
echo "==> Cleanup complete across ${count} hosts."
