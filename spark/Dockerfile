FROM ubuntu:22.04

# Install necessary packages
RUN apt-get update && \
    apt-get install -y wget python3 python3-pip openjdk-8-jre-headless scala && \
    wget https://downloads.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar -xvf spark-3.3.2-bin-hadoop3.tgz && \
    rm spark-3.3.2-bin-hadoop3.tgz && \
    mv spark-3.3.2-bin-hadoop3/ /spark && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Hadoop and configure it for pseudo-distributed mode
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz && \
    tar xzvf hadoop-3.3.4.tar.gz && \
    rm hadoop-3.3.4.tar.gz && \
    mv hadoop-3.3.4 /opt/hadoop && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_HOME=/opt/hadoop" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    sed -i '/<\/configuration>/i <property>\n<name>fs.defaultFS<\/name>\n<value>hdfs:\/\/localhost:9000<\/value>\n<\/property>\n<property>\n<name>hadoop.tmp.dir<\/name>\n<value>\/hadoop-data\/tmp<\/value>\n<\/property>' /opt/hadoop/etc/hadoop/core-site.xml && \
    sed -i '/<\/configuration>/i <property>\n<name>dfs.replication<\/name>\n<value>1<\/value>\n<\/property>\n<property>\n<name>dfs.namenode.name.dir<\/name>\n<value>\/hadoop-data\/dfs\/name<\/value>\n<\/property>\n<property>\n<name>dfs.datanode.data.dir<\/name>\n<value>\/hadoop-data\/dfs\/data<\/value>\n<\/property>' /opt/hadoop/etc/hadoop/hdfs-site.xml && \
    mkdir -p /hadoop-data/dfs/name && \
    mkdir -p /hadoop-data/dfs/data && \
    chmod -R 777 /hadoop-data && \
    /opt/hadoop/bin/hdfs namenode -format

# Install necessary libraries
RUN apt-get update && \
    apt-get install -y libhdf5-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir pandas numpy h5py
RUN pip3 install --no-cache-dir pyspark
RUN pip3 install tables
RUN pip3 install h5py

# Install Jupyter notebook
RUN pip3 install --no-cache-dir jupyter

# Set environment variables
ENV SPARK_HOME="/spark"
ENV SPARK_NO_DAEMONIZE="true"
ENV PATH $PATH:$SPARK_HOME/bin

# Expose ports for Jupyter notebook and Spark master web UI
EXPOSE 8080 7077 6066

# Copy start-spark.sh script
COPY start-spark.sh /start-spark.sh
# Start Jupyter notebook and Spark by default
CMD ["/bin/bash", "/start-spark.sh"]

COPY start-hadoop.sh /start-hadoop.sh
RUN chmod +x /start-hadoop.sh
