## Overview

This repository now supports a local Spark + Streamlit workflow for the Million Song Subset. The goal is to keep Apache Spark as the processing engine while replacing notebook tabs with a visible application that explains the analysis directly in the UI.

The original Docker and notebook material is still in the repo for reference, but the recommended local path is:

1. Create a Python virtual environment.
2. Install Spark-facing Python dependencies.
3. Point the Streamlit app at the extracted dataset folder or the original `.tar.gz` archive.
4. Run Spark in `local[*]` mode and explore the results in the app.

## Local App

The Streamlit app lives in [streamlit_app.py](streamlit_app.py), and the Spark ingestion plus analysis helpers live in [local_app/msd_analytics.py](local_app/msd_analytics.py).

The app shows:

* dataset coverage and timing metrics
* top songs and artists by `song_hotttnesss`
* yearly popularity trends
* explanation panels describing the pipeline, assumptions, and dataset gaps

## Local Setup

Use Python 3.11 for the local environment. The machine currently has Java 17, which is suitable for local Spark.

If you stay on Python 3.14, use PySpark 4.1 or newer. Spark 3.5.x can install on Python 3.14 but its Python RDD serialization path is not reliable there.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

If you want the app to open directly on your dataset, set an environment variable before launching:

```bash
export MSD_DATASET_SOURCE=/Users/vjaadhi2799/Downloads/millionsongsubset.tar.gz
streamlit run streamlit_app.py
```

If the source path is a `.tar.gz` archive, the app can extract it into `./data/` once and then reuse the extracted folder on later runs.

## Optional Standalone Spark Cluster

The default app mode is `local[*]`, which uses all local CPU cores from one machine. If you want to demonstrate Spark
standalone concepts such as a master and separate workers, you can run a local standalone cluster and point the app at it.

Spark's official standalone docs: https://spark.apache.org/docs/4.1.0-preview2/spark-standalone.html

This repo now includes helper scripts that use the PySpark distribution already installed inside `.venv`:

```bash
./scripts/start-standalone-cluster.sh
./scripts/status-standalone-cluster.sh
export SPARK_MASTER_URL=spark://127.0.0.1:7077
export SPARK_EXECUTOR_INSTANCES=2
export SPARK_EXECUTOR_CORES=3
export SPARK_EXECUTOR_MEMORY=2G
streamlit run streamlit_app.py
```

Or run the whole flow in one command:

```bash
./scripts/run-streamlit-standalone.sh
```

The helper scripts start:

* `1` Spark master on `spark://127.0.0.1:7077`
* `2` local workers on the same machine by default

You can stop the cluster with:

```bash
./scripts/stop-standalone-cluster.sh
```

Useful overrides:

* `SPARK_STANDALONE_WORKERS=2`
* `SPARK_STANDALONE_WORKER_CORES=3`
* `SPARK_STANDALONE_WORKER_MEMORY=2G`
* `SPARK_STANDALONE_MASTER_PORT=7077`

This is useful for showing Spark architecture and scheduling, but on one powerful machine it is usually not faster than `local[*]`.

## Dataset Notes

The current analyses are based on the Million Song Subset HDF5 files. The app reads metadata and year information from those files, builds a Spark DataFrame locally, and then renders the curated outputs in Streamlit.

The subset is large enough to justify Spark for ingestion, but small enough to explore comfortably on one machine. That makes local `venv` + Spark + Streamlit a better fit than Docker for day-to-day development.

## Legacy Material

The repository still includes the original notebook and Docker-based standalone cluster setup under `jupyter-notebooks/`, `spark/`, and `standalone/`. Those files are useful if you want to compare the local application against the earlier cluster-oriented version.
