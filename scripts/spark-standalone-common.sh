#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
STATE_DIR="${ROOT_DIR}/.spark-local"
RUN_DIR="${STATE_DIR}/run"
LOG_DIR="${STATE_DIR}/logs"
WORK_DIR="${STATE_DIR}/work"
VENV_PYTHON="${ROOT_DIR}/.venv/bin/python"
PROJECT_PYTHONPATH="${ROOT_DIR}${PYTHONPATH:+:${PYTHONPATH}}"

require_local_python() {
  if [ ! -x "${VENV_PYTHON}" ]; then
    echo "Missing ${VENV_PYTHON}. Create the repo venv first." >&2
    exit 1
  fi
}

resolve_spark_home() {
  "${VENV_PYTHON}" - <<'PY'
import pathlib
import pyspark

print(pathlib.Path(pyspark.__file__).resolve().parent)
PY
}

require_local_python
SPARK_HOME="${SPARK_HOME:-$(resolve_spark_home)}"
SPARK_CLASS="${SPARK_HOME}/bin/spark-class"

if [ ! -x "${SPARK_CLASS}" ]; then
  echo "Could not find spark-class at ${SPARK_CLASS}" >&2
  exit 1
fi

mkdir -p "${RUN_DIR}" "${LOG_DIR}" "${WORK_DIR}"

MASTER_HOST="${SPARK_STANDALONE_MASTER_HOST:-127.0.0.1}"
MASTER_PORT="${SPARK_STANDALONE_MASTER_PORT:-7077}"
MASTER_WEBUI_PORT="${SPARK_STANDALONE_MASTER_WEBUI_PORT:-8080}"
WORKER_COUNT="${SPARK_STANDALONE_WORKERS:-2}"
WORKER_BASE_PORT="${SPARK_STANDALONE_WORKER_BASE_PORT:-7081}"
WORKER_BASE_WEBUI_PORT="${SPARK_STANDALONE_WORKER_BASE_WEBUI_PORT:-8081}"
CPU_COUNT="$("${VENV_PYTHON}" - <<'PY'
import os
print(os.cpu_count() or 4)
PY
)"
DEFAULT_WORKER_CORES=$(( CPU_COUNT / (WORKER_COUNT + 1) ))
if [ "${DEFAULT_WORKER_CORES}" -lt 1 ]; then
  DEFAULT_WORKER_CORES=1
fi
WORKER_CORES="${SPARK_STANDALONE_WORKER_CORES:-${DEFAULT_WORKER_CORES}}"
WORKER_MEMORY="${SPARK_STANDALONE_WORKER_MEMORY:-2G}"
MASTER_URL="spark://${MASTER_HOST}:${MASTER_PORT}"

MASTER_PID_FILE="${RUN_DIR}/master.pid"
MASTER_LOG_FILE="${LOG_DIR}/master.log"

pid_is_running() {
  local pid="$1"
  kill -0 "${pid}" 2>/dev/null
}

read_pid() {
  local pid_file="$1"
  if [ -f "${pid_file}" ]; then
    tr -d '[:space:]' < "${pid_file}"
  fi
}

print_cluster_summary() {
  cat <<EOF
Spark standalone cluster configuration
  SPARK_HOME: ${SPARK_HOME}
  Master URL: ${MASTER_URL}
  Worker count: ${WORKER_COUNT}
  Worker cores each: ${WORKER_CORES}
  Worker memory each: ${WORKER_MEMORY}
  Logs: ${LOG_DIR}
EOF
}
