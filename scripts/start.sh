#!/bin/bash

$SPARK_HOME/sbin/start-all.sh

bin/mahout spark-itemsimilarity --input $2 --output $3 --master spark://$1:7077 --sparkExecutorMem 6g

$SPARK_HOME/sbin/stop-all.sh
