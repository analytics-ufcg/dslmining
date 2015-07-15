#!/bin/bash

###################################################################################################
# Script to start and stop spark                                                                   #
# This script must to be run inside the mahout folder                                              #
#                                                                                                  #
# To run this script you should pass the machine name which has spark as master, input file,       #     
# output folder                                                                                    #
# ex:                                                                                              #
#                                                                                                  #
#./start.sh hadoop-node-1 file out								   #
#./start.sh hadoop-node-1 file out 					                           #
#./start.sh hadoop-node-1 file out						                   #
###################################################################################################


$SPARK_HOME/sbin/start-all.sh

bin/mahout spark-itemsimilarity --master spark://$1:7077 --input $2 --output $3 --sparkExecutorMem 6g

$SPARK_HOME/sbin/stop-all.sh
