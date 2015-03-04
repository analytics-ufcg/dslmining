## MapReduce RecSys

Initial part of the recommendation process used by RecommenderJob class in Apache Mahout. At this stage, the data are transformed into user vectors and co-occurrence  items vectors. These vectors will be used later in the recommendation process.
To illustrate this initial phase, we can run a sample with data from wikipedia (data/links-simple-10).
The purpose of this example is to understand the MapReduce and how to implement it using Hadoop. More specifically, how to implement MapReduces nested classes, which the output of a MapReduce is the input to another.

## Execution

To run you must execute the script run.sh in the root of this folder:
```
sh run.sh [inputPath [outputPath]]
```
examples:

```
sh run.sh
```

```
sh run.sh "data/links-simple-1.txt" "data/wikipedia_output"
```


the inputPath and outputPath are optional. If only one value is passed, this value represents the inputPath.

## Data
### Description
The data contains links between articles form Wikipedia, in the following format:

```
articleIDA: articleIDB articleIDC articleIDD
```

```
1 : 3 4 5
```
On the above example, the article 1 has links to the 3, 4 and 5 articles.

### Files

On the data folder there are two files: links-simple-1.txt and links-simple-10.txt. They are smaller files of the file present in this links:

http://users.on.net/~henry/pagerank/links-simple-sorted.zip

the number of each file represent his size. For example, links-simple-1.txt has 1 MB.
