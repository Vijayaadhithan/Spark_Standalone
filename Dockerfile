FROM ubuntu:22.04
RUN apt-get update
RUN apt-get -y upgrade
RUN apt install -y openjdk-8-jre-headless
RUN apt install -y scala
RUN apt install -y wget
RUN apt-get install -y python3-pip
RUN apt install pip
RUN pip install notebook
RUN pip install jupyterlab
RUN pip install tables
RUN pip install h5py
RUN apt install -y screen
RUN wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
RUN tar xvf spark-3.3.2-bin-hadoop3.tgz
RUN mv spark-3.3.2-bin-hadoop3/ /usr/local/spark
ENV PATH="${PATH}:$SPARK_HOME/bin"
ENV SPARK_HOME="/usr/local/spark"
ENV SPARK_NO_DAEMONIZE="true"
#CMD $SPARK_HOME/sbin/start-master.sh & $SPARK_HOME/sbin/start-worker.sh spark://sparkmaster:7077
EXPOSE 8080 7077 6066
COPY start-spark.sh /
CMD ["/bin/bash", "start-spark.sh"]
