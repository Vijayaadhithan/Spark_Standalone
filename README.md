## Overview

This GitHub repository contains the source code and documentation for a scalable data processing solution designed and implemented for the Million Song Dataset. The project focuses on utilizing Apache Spark in a standalone cluster environment, analyzing the dataset's song attributes, and performing data operations on varying dataset sizes.

##Cluster Setup

We built a standalone cluster on a virtual machine with 16GB of RAM, 16 cores, running Ubuntu 22.04, and a 100GB attached volume. The cluster comprises a Jupyter Notebook container, Spark master and worker containers, and a mounted volume acting as a shared workspace with a Hadoop-distributed file system structure.

## Docker Image Hierarchy
* cluster-base: Base image with Ubuntu 22.04 and attached shared workspace.
* spark-base: Inherits properties from cluster-base, including Apache Spark installation.
* spark-master: Spark master image.
* spark-worker: Spark worker image.
* Jupiter-notebook: Jupyter notebook image.
A shell script is provided to build all images at once, and Docker Compose is set up using these images.

## Dataset Scaling

The original dataset includes 10,000 songs (2.6GB). We replicated and generated datasets of different sizes (10k, 20k, 30k, 40k, 50k songs) to test the solution's scalability.

## Experiments

Spark Worker Configuration
After scaling to two workers, both simple and complex operation times decreased by half compared to a single worker. However, the loading time into the Spark DataFrame was not significantly reduced.

## Data Analysis on Songs Attribute
We focused on the songs attribute within the metadata group, performing analyses such as song count and identifying the most popular songs based on the hotness feature.

## Popular Songs Analysis
A bar plot was created to display the highest popularity for each year, showcasing years with songs like "Immigrant Song" in 1970 and "Nothin’ On You [feat. Bruno Mars]" in 2010.

## Conclusion and Recommendations

* Optimal Spark Worker Configuration: Two Spark workers are recommended in the current settings (10,000 songs - 50,000 songs) considering code size and human resources.
* Dataset Size Considerations: Due to hardware limitations, processing a million songs wasn't feasible. Future work could explore handling larger datasets and optimizing the Spark setup further.
* Code Efficiency and Resource Utilization: The performance is better with 2, 3, and 4 workers than with only 1 worker, emphasizing the efficiency of the Spark cluster.
