#initialize input and output variables
inputPath=""
outputPath=""

#if the inputPath is not passed, uses a default value
if test -z "$1"
	then inputPath="data/words.txt"
	else inputPath=$1
fi

#if the outputPath is not passed, uses a default value
if test -z "$2"
	then outputPath="data"
	else outputPath=$2
fi
outputPath="$outputPath/wordcount-output"
#run the main class of the project using activator. 
./activator "run $inputPath $outputPath"

#sort the generated file by value (number of times that the word is present). The original file is sorted by the key (word)
sort -nrk2 "$outputPath/part-r-00000" > "$outputPath/sorted.txt"

