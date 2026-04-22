#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/spark-standalone-common.sh"

stop_process() {
  local label="$1"
  local pid_file="$2"
  local pid

  pid="$(read_pid "${pid_file}")"
  if [ -z "${pid}" ]; then
    echo "${label}: not running"
    rm -f "${pid_file}"
    return
  fi

  if ! pid_is_running "${pid}"; then
    echo "${label}: removing stale PID ${pid}"
    rm -f "${pid_file}"
    return
  fi

  kill "${pid}" 2>/dev/null || true
  for _ in $(seq 1 20); do
    if ! pid_is_running "${pid}"; then
      rm -f "${pid_file}"
      echo "${label}: stopped"
      return
    fi
    sleep 0.5
  done

  kill -9 "${pid}" 2>/dev/null || true
  rm -f "${pid_file}"
  echo "${label}: force-stopped"
}

for index in $(seq "${WORKER_COUNT}" -1 1); do
  stop_process "Worker ${index}" "${RUN_DIR}/worker-${index}.pid"
done

stop_process "Master" "${MASTER_PID_FILE}"

