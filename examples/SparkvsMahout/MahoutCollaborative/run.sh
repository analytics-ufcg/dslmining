#! / bin / bash 

#initialize input and output variables
inputPath=""

sed -re 's/::+/,/g' < "$1" > data/temp.txt 
cut -d',' -f -3 < data/temp.txt > data/ratings.txt
rm data/temp.txt

inputPath="data/ratings.txt"

./activator "run $inputPath"

