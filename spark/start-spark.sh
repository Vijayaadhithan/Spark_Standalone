#!/bin/bash

export SPARK_HOME="/spark"
export PATH="${PATH}:$SPARK_HOME/bin"
export SPARK_NO_DAEMONIZE="true"
. "${SPARK_HOME}/sbin/load-spark-env.sh"

# Start HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# Start Spark master
${SPARK_HOME}/sbin/start-master.sh

# Start Spark worker
${SPARK_HOME}/sbin/start-worker.sh spark://spark-master:7077

# Start Jupyter notebook
jupyter-notebook --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token=''
