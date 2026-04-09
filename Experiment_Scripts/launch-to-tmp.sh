#!/usr/bin/env bash
# =============================================================================
# launch-to-tmp.sh — Launch one Java process per virtual IP across cluster nodes
#
# Reads a net-map file (format: "<hostname> <virtual-ip>" one per line) and
# SSH-es into each host to start a background Java process bound to that IP.
#
# Usage:  ./launch-to-tmp.sh [net-map-file]
#         Default net-map file: net-map.txt in the same directory as this script
# =============================================================================

set -uo pipefail

# -----------------------------------------------------------------------------
# REQUIRED — you MUST set these before running
# -----------------------------------------------------------------------------

# Your group's home directory on the cluster (this is your shared network volume).
# Replace XX with your group number, e.g. /home/distalg03
SHARED_DIR="/home/distalg06"

# Path to your JAR file (relative to SHARED_DIR, adjust filename as needed)
JAR_PATH="${SHARED_DIR}/distalg-project1-base/target/DistAlg.jar"

# TCP port all processes will listen on
BABEL_PORT="5000"

# Name of the contact argument your protocol expects (e.g. "HyParView.contact",
# "Membership.contact", or whatever your implementation uses)
CONTACT_PARAM="contact"

# Value passed as the contact argument to the FIRST node (the bootstrap node).
# If your implementation expects no contact argument at all for the first node,
# leave this empty: CONTACT_FIRST_VALUE=""
# If it expects a special token (e.g. "none", "bootstrap"), set it here.
CONTACT_FIRST_VALUE=""

# Arguments passed to EVERY node (same value across all processes).
# babel.address and babel.port are added automatically — do not repeat them here.
# Example: COMMON_ARGS="Membership=HyParView Dissemination=PlumTree logLevel=info"
COMMON_ARGS=""

# -----------------------------------------------------------------------------
# OPTIONAL — safe to leave as-is, adjust only if needed
# -----------------------------------------------------------------------------

# Path to the Java installation on the REMOTE machines
JAVA_HOME_REMOTE="/usr/lib/jvm/jdk-22-oracle-x64"

# Local (per-machine) directory where logs are written during the experiment.
# Using /tmp avoids hammering the network mount during the run.
# Each process gets its own subdirectory: $LOG_BASE_DIR_LOCAL/<ip>/
LOG_BASE_DIR_LOCAL="/tmp/distalg-logs"

# Network mount where logs are collected at the end of the experiment.
# Run collect-logs.sh after the experiment to copy everything here.
LOG_BASE_DIR_NETWORK="${SHARED_DIR}/logs"

# Extra JVM flags (e.g. heap size, GC options). Leave empty if none.
JVM_FLAGS="-Xmx800m -Xms256m"

# Seconds to sleep between launching each process.
# Increase this if the cluster's SSH jump host rejects connections under load.
LAUNCH_DELAY=2

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

JAVA_BIN="${JAVA_HOME_REMOTE}/bin/java"

# Count only non-blank, non-comment lines
total=$(grep -c '[^[:space:]]' "$NET_MAP" || true)
count=0
first_ip=""

echo "==> Launching Java processes from net-map: $NET_MAP"
echo "==> Total entries:  $total"
echo "==> JAR:            $JAR_PATH"
echo "==> JAVA_HOME:      $JAVA_HOME_REMOTE"
echo "==> Babel port:     $BABEL_PORT"
echo "==> Contact param:  $CONTACT_PARAM"
echo "==> Launch delay:   ${LAUNCH_DELAY}s between processes"
echo "==> Local log dir:  $LOG_BASE_DIR_LOCAL"
echo "==> Network logs:   $LOG_BASE_DIR_NETWORK  (collected after experiment)"
echo ""

while IFS=" " read -r host ip; do
    # Skip blank lines or comments
    [[ -z "$host" || "$host" == \#* ]] && continue

    count=$((count + 1))

    # Build the SSH target (optionally prefix user@)
    if [[ -n "$SSH_USER" ]]; then
        SSH_TARGET="${SSH_USER}@${host}"
    else
        SSH_TARGET="${host}"
    fi

    # Per-process log directory and log file — on local /tmp
    LOG_DIR="${LOG_BASE_DIR_LOCAL}/${ip}"
    LOG_FILE="${LOG_DIR}/app.log"

    # Build the contact argument:
    #   - First node:  either omitted (if CONTACT_FIRST_VALUE is empty) or
    #                  set to CONTACT_FIRST_VALUE (e.g. a special bootstrap token)
    #   - Other nodes: CONTACT_PARAM=<first-node-ip>:<port>
    if [[ $count -eq 1 ]]; then
        first_ip="${ip}"
        if [[ -n "$CONTACT_FIRST_VALUE" ]]; then
            CONTACT_ARG="${CONTACT_PARAM}=${CONTACT_FIRST_VALUE}"
        else
            CONTACT_ARG=""
        fi
        echo "[${count}/${total}] ${host} — IP: ${ip}  *** FIRST NODE (bootstrap) ***"
    else
        CONTACT_ARG="${CONTACT_PARAM}=${first_ip}:${BABEL_PORT}"
        echo "[${count}/${total}] ${host} — IP: ${ip}"
    fi

    # The remote command:
    #   1. Export JAVA_HOME so the correct JDK is used
    #   2. Create and cd into the per-process log directory (on local /tmp)
    #   3. Launch detached from the terminal using nohup + & + disown
    REMOTE_CMD="export JAVA_HOME='${JAVA_HOME_REMOTE}'; \
mkdir -p '${LOG_DIR}'; \
cd '${LOG_DIR}'; \
nohup '${JAVA_BIN}' ${JVM_FLAGS} -jar '${JAR_PATH}' \
  ${COMMON_ARGS} \
  babel.address='${ip}' \
  babel.port='${BABEL_PORT}' \
  ${CONTACT_ARG} \
  >> '${LOG_FILE}' 2>&1 </dev/null & \
disown \$!"

    # shellcheck disable=SC2086
    if ssh ${SSH_OPTS} "$SSH_TARGET" "$REMOTE_CMD"; then
        echo "              ✓ launched (log: ${LOG_FILE})"
    else
        echo "              ✗ FAILED on ${host} for IP ${ip}" >&2
    fi

    # Small delay to avoid hammering the jump host
    if [[ "$LAUNCH_DELAY" != "0" && $count -lt $total ]]; then
        sleep "$LAUNCH_DELAY"
    fi

done < "$NET_MAP"

echo ""
echo "==> Done. ${count} processes launched."
echo "==> First node IP (contact for all others): ${first_ip}"
echo ""
echo "==> When the experiment is finished, run collect-logs.sh to copy logs to the network mount."
