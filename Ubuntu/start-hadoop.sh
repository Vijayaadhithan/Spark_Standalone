#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Format namenode (only run once)
if [ ! -f /tmp/hadoop-hdfs/dfs/name/current/VERSION ]; then
  $HADOOP_HOME/bin/hdfs namenode -format
fi

# Start Hadoop
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Keep container running
tail -f /dev/null
