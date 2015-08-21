#!/bin/bash
COUNTER=0
while [  $COUNTER -lt $1 ]; do

   ssh -i cloud_analytics.pem ubuntu@ssh.cloud1.lsd.ufcg.edu.br -p 10012 'ssh hadoop-node-9 "sudo reboot"; ssh hadoop-node-8 "sudo reboot"; ssh hadoop-node-7 "sudo reboot"; sudo reboot' 
   
   sleep 5m

   ssh -i cloud_analytics.pem ubuntu@ssh.cloud1.lsd.ufcg.edu.br -p 10012 'ssh hadoop-node-3 "rm -r /usr/local/mahout/out*; cd /usr/local/mahout; ./runSpark-item-similarity.sh -m hadoop-node-3 -i ~/wiki_data_10m.txt -o out10$COUNTER -s hadoop-node-7 -s hadoop-node-8 -s hadoop-node-9" >> /home/ubuntu/ciclo1-clusterC10.out'

   echo "Fim loop $COUNTER de 10M"

   let COUNTER=COUNTER+1
done


COUNTER=0
while [  $COUNTER -lt $1 ]; do

   ssh -i cloud_analytics.pem ubuntu@ssh.cloud1.lsd.ufcg.edu.br -p 10012 'ssh hadoop-node-9 "sudo reboot"; ssh hadoop-node-8 "sudo reboot"; ssh hadoop-node-7 "sudo reboot"; sudo reboot' 
   
   sleep 5m

   ssh -i cloud_analytics.pem ubuntu@ssh.cloud1.lsd.ufcg.edu.br -p 10012 'ssh hadoop-node-3 "rm -r /usr/local/mahout/out*; cd /usr/local/mahout; ./runSpark-item-similarity.sh -m hadoop-node-3 -i ~/wiki_data_25m.txt -o out25$COUNTER -s hadoop-node-7 -s hadoop-node-8 -s hadoop-node-9" >> /home/ubuntu/ciclo1-clusterC25.out'

   echo "Fim loop $COUNTER de 25M"

   let COUNTER=COUNTER+1
done


COUNTER=0
while [  $COUNTER -lt $1 ]; do

   ssh -i cloud_analytics.pem ubuntu@ssh.cloud1.lsd.ufcg.edu.br -p 10012 'ssh hadoop-node-9 "sudo reboot"; ssh hadoop-node-8 "sudo reboot"; ssh hadoop-node-7 "sudo reboot"; sudo reboot' 
   
   sleep 5m

   ssh -i cloud_analytics.pem ubuntu@ssh.cloud1.lsd.ufcg.edu.br -p 10012 'ssh hadoop-node-3 "rm -r /usr/local/mahout/out*; cd /usr/local/mahout; ./runSpark-item-similarity.sh -m hadoop-node-3 -i ~/wiki_data_50m.txt -o out50$COUNTER -s hadoop-node-7 -s hadoop-node-8 -s hadoop-node-9" >> /home/ubuntu/ciclo1-clusterC50.out'

   echo "Fim loop $COUNTER de 50M"

   let COUNTER=COUNTER+1
done


COUNTER=0
while [  $COUNTER -lt $1 ]; do

   ssh -i cloud_analytics.pem ubuntu@ssh.cloud1.lsd.ufcg.edu.br -p 10012 'ssh hadoop-node-9 "sudo reboot"; ssh hadoop-node-8 "sudo reboot"; ssh hadoop-node-7 "sudo reboot"; sudo reboot' 
   
   sleep 5m

   ssh -i cloud_analytics.pem ubuntu@ssh.cloud1.lsd.ufcg.edu.br -p 10012 'ssh hadoop-node-3 "rm -r /usr/local/mahout/out*; cd /usr/local/mahout; ./runSpark-item-similarity.sh -m hadoop-node-3 -i ~/wiki_data_100m.txt -o out100$COUNTER -s hadoop-node-7 -s hadoop-node-8 -s hadoop-node-9" >> /home/ubuntu/ciclo1-clusterC100.out'

   echo "Fim loop $COUNTER de 100M"

   let COUNTER=COUNTER+1
done
