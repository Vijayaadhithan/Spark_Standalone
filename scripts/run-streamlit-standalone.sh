#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

"${SCRIPT_DIR}/start-standalone-cluster.sh"

source "${SCRIPT_DIR}/spark-standalone-common.sh"

export SPARK_MASTER_URL="${MASTER_URL}"
export SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-${WORKER_COUNT}}"
export SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-${WORKER_CORES}}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-${WORKER_MEMORY}}"
export SPARK_CORES_MAX="${SPARK_CORES_MAX:-$((WORKER_COUNT * WORKER_CORES))}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-${VENV_PYTHON}}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-${VENV_PYTHON}}"
export PYTHONPATH="${PROJECT_PYTHONPATH}"

exec "${VENV_PYTHON}" -m streamlit run "${ROOT_DIR}/streamlit_app.py" "$@"
