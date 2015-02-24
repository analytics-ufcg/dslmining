outputPath = ""

if [ -z "$2"]
	then outputPath = "data/wordcount"
	else outputPath = $2


./activator "run $1 $2"
sort -nrk2 "data/wordcount/part-r-00000" > sorted.txt

