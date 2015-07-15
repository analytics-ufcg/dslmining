#!/bin/bash

########################################################################
# Script to run mahout with spark                                       #
# O script deve ser rodado dentro da pasta mahout                       #
# Para executar o script é necessário informar o arquivo de entrada,    #
# a pasta de saida e os nomes dos slaves.                               #
# Caso não seja passado slaves então ele roda localmente.               #
#                                                                       #
# ex:                                                                   #
#                                                                       #
#sh runSpark.sh file out hadoop-node-4 hadoop-node-3                    #
#sh runSpark.sh file out hadoop-node-3                                  #
#sh runSpark.sh file out                                                #
#########################################################################


start=`date +%s`

rm -r $SPARK_HOME/conf/slaves

if [ -n "$3" ]; then
    echo $3 >> $SPARK_HOME/conf/slaves
    echo $4 >> $SPARK_HOME/conf/slaves
else
    echo "localhost"  >> $SPARK_HOME/conf/slaves
fi

sh startRun.sh $1 $2

end=`date +%s`

runtime=$((end-start))

echo "Total time  was $runtime"

