FROM cluster-base

# -- Layer: Apache Spark

ARG spark_version=3.3.2
ARG hadoop_version=3

RUN apt-get update -y && \
    apt-get -y upgrade && \
    apt install -y openjdk-8-jre-headless && \
    apt install -y scala &&\
    apt install -y wget &&\
    apt install -y screen &&\
    wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar xvf spark-3.3.2-bin-hadoop3.tgz && \
    mv spark-3.3.2-bin-hadoop3 /usr/bin/ && \
    mkdir /usr/bin/spark-3.3.2-bin-hadoop3/logs

ENV SPARK_HOME /usr/bin/spark-3.3.2-bin-hadoop3
ENV SPARK_MASTER_HOST  spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON  python3

# -- Runtime

WORKDIR ${SPARK_HOME}
