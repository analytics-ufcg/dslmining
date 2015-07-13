#!/bin/bash

###################################################################################################
# Script to run mahout com spark                                       			           #
# O script deve ser rodado dentro da pasta mahout                       			   #
#												   #
# Para executar o script é necessário informar o nome da maquina que tem			   #	 
# o spark como master, o arquivo de entrada,    						   #
# a pasta de saida e os nomes dos slaves.                               			   #
# Caso não seja passado slaves então ele roda localmente.               			   #
#                                                                       			   #
# ex:                                                                   			   #
#                                                                       		  	   #
#sh runSpark-item-similarity.sh -m hadoop-node-1 -i file -o out -s hadoop-node-4 -s hadoop-node-3  #
#sh runSpark-item-similarity.sh -m hadoop-node-1 -i file -o out					   #
#sh runSpark-item-similarity.sh -m hadoop-node-1 -i file -o out -s hadoop-node-3                   #
###################################################################################################

start=`date +%s`

SLAVE1=""
SLAVE2=""
SPARK_MASTER=""
INPUT=""
OUTPUT=""

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
	if [ -z "$SLAVE1" ]; then
		SLAVE1=$OPTARG
	else
	        SLAVE2=$OPTARG
	fi
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

if [ -z "$SLAVE1" ]; then # verifica se algum slave foi passado
    echo "localhost"  >> $SPARK_HOME/conf/slaves
else
    echo $SLAVE1 >> $SPARK_HOME/conf/slaves
    echo $SLAVE2 >> $SPARK_HOME/conf/slaves
fi

sh start.sh $SPARK_MASTER $INPUT $OUTPUT

end=`date +%s`

runtime=$((end-start))

echo "Total time  was $runtime"

