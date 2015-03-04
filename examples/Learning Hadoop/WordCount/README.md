#Word Count
Example of Word Count utilizing Hadoop Map Reduce. The program receives a file and counts the number of occurrences of each word. The default file is on "data" folder. This file is an excerpt from the Game of Thrones book.

##Execution
To run you must execute the script run.sh in the root of this folder:
```
sh run.sh [inputPath [outputPath]]
```
examples:
```
sh run.sh
```

```
sh run.sh "data/words.txt" "data/wordcount"
```


the inputPath and outputPath are optional. If only one value is passed, this value represents the inputPath.
