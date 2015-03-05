#!/bin/bash 

if [ $# -ne 0 	]; then
	sed -re 's/::+/,/g' < "$1" > data/temp.txt 
	cut -d',' -f -3 < data/temp.txt > data/ratings.txt
	rm data/temp.txt
fi

