FROM ubuntu:22.04

# Install necessary packages
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://downloads.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar -xvf spark-3.3.2-bin-hadoop3.tgz && \
    rm spark-3.3.2-bin-hadoop3.tgz && \
    mv spark-3.3.2-bin-hadoop3 /spark && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install pandas, numpy, and h5py packages separately
RUN pip3 install --no-cache-dir pandas
RUN pip3 install --no-cache-dir numpy
RUN pip3 install --no-cache-dir h5py

# Install the tables package separately
RUN pip3 install tables

# Install Jupyter notebook
RUN pip3 install --no-cache-dir jupyter

# Set environment variables
ENV SPARK_HOME /spark
ENV PATH $PATH:$SPARK_HOME/bin

# Expose ports for Jupyter notebook and Spark master web UI
EXPOSE 8888 8080

# Start Jupyter notebook by default
CMD ["/home/ubuntu/start-notebook.sh"]


