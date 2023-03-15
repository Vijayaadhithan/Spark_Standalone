#!/bin/bash
export SPARK_HOME="/usr/local/spark"
export PATH="${PATH}:$SPARK_HOME/bin"
export SPARK_NO_DAEMONIZE="true"
. "${SPARK_HOME}/sbin/load-spark-env.sh"

#When the spark work_load is master run class org.apache.spark.deploy.master.Master


if [ "$SPARK_WORKLOAD" == "master" ];
then

${SPARK_HOME}/sbin/start-master.sh

elif [ "$SPARK_WORKLOAD" == "worker" ];
then

${SPARK_HOME}/sbin/start-worker.sh spark://spark-master:7077
#When the spark work_load is worker run class org.apache.spark.deploy.master.Worker

#elif [ "$SPARK_WORKLOAD" == "submit" ];
#then
#echo "SPARK SUBMIT"
#else
#echo "Undefined Workload Type $SPARK_WORKLOAD, must specify: master, worker, submit"
fi

#Start master and worker

