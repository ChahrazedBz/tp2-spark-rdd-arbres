# TP2 – Apache Spark RDD: Analysis of Tree Data

## Overview
This practical assignment introduces Apache Spark and its core abstraction, the Resilient Distributed Dataset (RDD). It consists of deploying a Spark cluster on Docker containers and writing a PySpark program that analyzes a dataset of trees (`arbres.csv`) stored in HDFS.

The program relies on RDD transformations and actions (`map`, `filter`, `sortByKey`, `reduceByKey`, `count`, `mean`) to extract statistics from the data in a distributed way.

## Technologies
* Apache Spark (standalone cluster mode)
* PySpark (Python)
* RDD API
* Hadoop Distributed File System (HDFS)
* Docker

## Spark Cluster Architecture
The cluster consists of three Docker containers connected by a Docker bridge network (same image as TP1):

* `hadoop-master`: Spark Master (port 8080 exposed for the web UI) and HDFS NameNode.
* `hadoop-slave1`: Spark Worker and HDFS DataNode.
* `hadoop-slave2`: Spark Worker and HDFS DataNode.

Spark is configured by listing the worker container names in the `slaves` file under `/usr/local/spark/conf`, then starting the Spark services on all nodes.

## Project Structure
```
.
├── arbres_rdd.py     # PySpark program (RDD analysis)
└── README.md
```

## Implementation
The program loads `arbres.csv` from HDFS, separates the header from the data, locates the `hauteur` and `genre` columns by name, and performs four tasks:

1. **RDD creation and line count**: builds an RDD from the file and counts its lines.
2. **Average tree height**: extracts the heights, filters invalid values, and computes the mean.
3. **Genus of the tallest tree**: builds `(height, genus)` pairs, sorts them by descending key with `sortByKey`, and keeps the first pair.
4. **Number of trees per genus**: builds `(genus, 1)` pairs and aggregates them with `reduceByKey`.

## Execution Workflow
1. Create the Docker network and launch the three containers (port 8080 exposed on the master).
2. Create the `slaves` configuration file and add the worker names.
3. Start HDFS, then the Spark master and workers.
4. Upload `arbres.csv` to HDFS (`/user/root/arbres/`).
5. Submit the program to the cluster:
   ```bash
   spark-submit --master spark://hadoop-master:7077 arbres_rdd.py
   ```
6. Check the results in the console and the job in the Spark web UI (port 8080).

## Learning Outcomes
* Deploying a multi-node Spark cluster with Docker.
* Understanding the RDD model, lazy transformations, and actions.
* Using key/value RDDs (`sortByKey`, `reduceByKey`) for aggregation and ranking.
* Reading data from HDFS and submitting PySpark jobs to a cluster.
* Comparing Spark's in-memory processing with Hadoop MapReduce (TP1).

## Academic Context
Practical Work 2 – Big Data
Higher School of Computer Science (ESI-SBA), Algeria
Academic Year: 2026–2027