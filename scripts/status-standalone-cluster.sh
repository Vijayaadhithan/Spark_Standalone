#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/spark-standalone-common.sh"

show_status() {
  local label="$1"
  local pid_file="$2"
  local pid

  pid="$(read_pid "${pid_file}")"
  if [ -n "${pid}" ] && pid_is_running "${pid}"; then
    echo "${label}: running (PID ${pid})"
  elif [ -n "${pid}" ]; then
    echo "${label}: stale PID file (${pid})"
  else
    echo "${label}: stopped"
  fi
}

print_cluster_summary
show_status "Master" "${MASTER_PID_FILE}"
for index in $(seq 1 "${WORKER_COUNT}"); do
  show_status "Worker ${index}" "${RUN_DIR}/worker-${index}.pid"
done

cat <<EOF
Master UI: http://${MASTER_HOST}:${MASTER_WEBUI_PORT}
Expected worker UIs start at: http://${MASTER_HOST}:${WORKER_BASE_WEBUI_PORT}
EOF

