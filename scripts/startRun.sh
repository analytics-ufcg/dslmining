#!/bin/bash

$SPARK_HOME/sbin/start-all.sh

bin/mahout spark-itemsimilarity --input $1 --output $2 --master spark://hadoop-node-1:7077 --sparkExecutorMem 6g

$SPARK_HOME/sbin/stop-all.sh
