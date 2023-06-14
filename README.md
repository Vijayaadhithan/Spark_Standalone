# Apache Spark Standalone Cluster on Docker


This project gives you an Apache Spark cluster in standalone mode with a JupyterLab interface built on top of Docker. Learn Apache Spark through its Scala, Python (PySpark) and R (SparkR) API by running the Jupyter notebooks with examples on how to read, process and write data.




Cluster overview

| Application   | URL               | Description                                 |
| ------------- | ----------------- | ------------------------------------------- |
| JupyterLab    | localhost:8888    | Cluster interface with built-in Jupyter notebooks |
| Spark Driver  | localhost:4040    | Spark Driver web ui                          |
| Spark Master  | localhost:8080    | Spark Master node                            |
| Spark Worker I| localhost:8081    | Spark Worker node with 1 core and 512m of memory (default) |
| Spark Worker II| localhost:8082   | Spark Worker node with 1 core and 512m of memory (default) |
