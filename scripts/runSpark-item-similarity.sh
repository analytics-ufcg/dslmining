#!/bin/bashi

###################################################################################################
# Script to run mahout with spark                                     			           #
# This script must to be run inside the mahout folder                       			   #
#												   #
# To run this script you should pass the machine name which has spark as master, input file,       #
# output folder, machines slaves(opt)                                    			   #
# In case where machines slaves are not pass as parameter, spark will run as localhost		   #
#                                                                       			   #
# ex:                                                                   			   #
#                                                                       		  	   #
#./runSpark-item-similarity.sh -m hadoop-node-1 -i file -o out -s hadoop-node-4 -s hadoop-node-3   #
#./runSpark-item-similarity.sh -m hadoop-node-1 -i file -o out					   #
#./runSpark-item-similarity.sh -m hadoop-node-1 -i file -o out -s hadoop-node-3                    #
###################################################################################################

start=`date +%s`

SPARK_MASTER=""
INPUT=""
OUTPUT=""
I=0

# Set all variables
while getopts ":m:i:o:s:" opt; do
   case $opt in
   m)
	SPARK_MASTER=$OPTARG
	;;
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

rm -r $SPARK_HOME/conf/slaves

# Check if slave is empty
if [ -z "$SLAVE" ]; then  
    echo "localhost"  >> $SPARK_HOME/conf/slaves
else
    for i in "${ARRAY_SLAVES[@]}"
    do
	echo $i >> $SPARK_HOME/conf/slaves
    done
fi

# Run start.sh script
./start.sh $SPARK_MASTER $INPUT $OUTPUT

end=`date +%s`

runtime=$((end-start))

echo "Total time  was $runtime"

