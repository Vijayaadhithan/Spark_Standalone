#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/spark-standalone-common.sh"

start_master() {
  local existing_pid
  existing_pid="$(read_pid "${MASTER_PID_FILE}")"
  if [ -n "${existing_pid}" ] && pid_is_running "${existing_pid}"; then
    echo "Master already running with PID ${existing_pid}"
    return
  fi

  rm -f "${MASTER_PID_FILE}"
  mkdir -p "${WORK_DIR}/master-local"

  nohup env \
    SPARK_HOME="${SPARK_HOME}" \
    SPARK_NO_DAEMONIZE=1 \
    SPARK_LOCAL_DIRS="${WORK_DIR}/master-local" \
    PYSPARK_PYTHON="${VENV_PYTHON}" \
    PYSPARK_DRIVER_PYTHON="${VENV_PYTHON}" \
    PYTHONPATH="${PROJECT_PYTHONPATH}" \
    "${SPARK_CLASS}" \
    org.apache.spark.deploy.master.Master \
    --host "${MASTER_HOST}" \
    --port "${MASTER_PORT}" \
    --webui-port "${MASTER_WEBUI_PORT}" \
    >"${MASTER_LOG_FILE}" 2>&1 &

  echo $! > "${MASTER_PID_FILE}"
  sleep 2
  if ! pid_is_running "$(read_pid "${MASTER_PID_FILE}")"; then
    echo "Failed to start Spark master. Check ${MASTER_LOG_FILE}" >&2
    exit 1
  fi
  echo "Started Spark master at ${MASTER_URL}"
}

start_worker() {
  local index="$1"
  local worker_port=$((WORKER_BASE_PORT + index - 1))
  local worker_webui_port=$((WORKER_BASE_WEBUI_PORT + index - 1))
  local worker_pid_file="${RUN_DIR}/worker-${index}.pid"
  local worker_log_file="${LOG_DIR}/worker-${index}.log"
  local worker_dir="${WORK_DIR}/worker-${index}"
  local worker_local_dir="${WORK_DIR}/local-${index}"
  local existing_pid

  existing_pid="$(read_pid "${worker_pid_file}")"
  if [ -n "${existing_pid}" ] && pid_is_running "${existing_pid}"; then
    echo "Worker ${index} already running with PID ${existing_pid}"
    return
  fi

  rm -f "${worker_pid_file}"
  mkdir -p "${worker_dir}" "${worker_local_dir}"

  nohup env \
    SPARK_HOME="${SPARK_HOME}" \
    SPARK_NO_DAEMONIZE=1 \
    SPARK_WORKER_DIR="${worker_dir}" \
    SPARK_LOCAL_DIRS="${worker_local_dir}" \
    PYSPARK_PYTHON="${VENV_PYTHON}" \
    PYSPARK_DRIVER_PYTHON="${VENV_PYTHON}" \
    PYTHONPATH="${PROJECT_PYTHONPATH}" \
    "${SPARK_CLASS}" \
    org.apache.spark.deploy.worker.Worker \
    --host "${MASTER_HOST}" \
    --port "${worker_port}" \
    --webui-port "${worker_webui_port}" \
    --cores "${WORKER_CORES}" \
    --memory "${WORKER_MEMORY}" \
    "${MASTER_URL}" \
    >"${worker_log_file}" 2>&1 &

  echo $! > "${worker_pid_file}"
  sleep 2
  if ! pid_is_running "$(read_pid "${worker_pid_file}")"; then
    echo "Failed to start worker ${index}. Check ${worker_log_file}" >&2
    exit 1
  fi
  echo "Started worker ${index} on web UI port ${worker_webui_port}"
}

print_cluster_summary
start_master

for index in $(seq 1 "${WORKER_COUNT}"); do
  start_worker "${index}"
done

cat <<EOF

Standalone Spark cluster is running.
  Master UI: http://${MASTER_HOST}:${MASTER_WEBUI_PORT}
  Master URL: ${MASTER_URL}

To run the Streamlit app against this cluster:
  export SPARK_MASTER_URL=${MASTER_URL}
  export SPARK_EXECUTOR_INSTANCES=${WORKER_COUNT}
  export SPARK_EXECUTOR_CORES=${WORKER_CORES}
  export SPARK_EXECUTOR_MEMORY=${WORKER_MEMORY}
  export SPARK_CORES_MAX=$((WORKER_COUNT * WORKER_CORES))
  ${VENV_PYTHON} -m streamlit run ${ROOT_DIR}/streamlit_app.py
EOF
