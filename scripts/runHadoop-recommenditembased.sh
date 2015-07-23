#!/bin/bash

###################################################################################################
# Script to run mahout with hadoop                                     			           
# This script must to be run inside the mahout folder                       			   
#												   
# To run this script you should pass:
# input file(-i), 
# output folder(-o), 
# machines slaves(-s), in case where machines slaves are not pass as parameter spark will run as localhost
#                                                                       			   
# ex:                                                                   			   
#                                                                       		  	   
#./runHadoop-recommenditembased.sh -i file -o out -s hadoop-node-4 -s hadoop-node-3   
#./runHadoop-recommenditembased.sh -i file -o out					   
#./runHadoop-recommenditembased.sh -i file -o out -s hadoop-node-3                    
###################################################################################################

start=`date +%s`

INPUT=""
OUTPUT=""
I=0

# Set all variables
while getopts ":i:o:s:" opt; do
   case $opt in
   i)
	INPUT=$OPTARG
	;;
   o)
	OUTPUT=$OPTARG
	;;
   s)	
	SLAVE=$OPTARG
	ARRAY_SLAVES[I]=$SLAVE
	I=$I+1
	;;					
   \?)
	echo "Invalid option: -$OPTARG" >&2
	exit 1
	;;
    :)
	echo "Option -$OPTARG requires an argument" >&2
	exit 1
	;;
   esac
done


rm -r $HADOOP_HOME/etc/hadoop/slaves

# Add the namenodes to the slaves file
if [ -z "$SLAVE" ]; then  
    echo "localhost"  >> $HADOOP_HOME/etc/hadoop/slaves
else
    for i in "${ARRAY_SLAVES[@]}"
    do
	echo $i >> $HADOOP_HOME/etc/hadoop/slaves
    done
fi

$HADOOP_HOME/sbin/start-all.sh 

$HADOOP_HOME/bin/hdfs dfs -rmr temp

# TODO Add replication number 

bin/mahout recommenditembased -s SIMILARITY_LOGLIKELIHOOD -i $INPUT -o $OUTPUT

$HADOOP_HOME/sbin/stop-all.sh 

end=`date +%s`

runtime=$((end-start))

echo "Total time  was $runtime seconds"
echo "Total time  was $((runtime/60)) minutes"
