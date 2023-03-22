#ARG debian_buster_image_tag=8-jre-slim
FROM ubuntu:22.04

# -- Layer: OS + Python 3.7

ARG shared_workspace=/opt/workspace

RUN mkdir -p ${shared_workspace} && \
    apt-get update -y && \
    apt-get -y upgrade &&\
    apt-get install -y apt-utils python3  python3-pip net-tools openjdk-11-jdk-headless&& \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install PyArrow h5py pandas notebook pyspark==3.3.2
RUN pip3 install matplotlib
ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
CMD ["bash"]
