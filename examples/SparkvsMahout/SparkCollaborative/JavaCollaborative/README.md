## Java Collaborative Filtering with Spark

Implementation in java of collaborative filtering using Spark.

##Execution
In order to execute the script you have to run the the script run.sh inside in the root of this folder.
example:
	./run.sh [inputPath]

InputPath is optional and represents the dataset path. If there is no inputPath the program is going to use the "data/ml-100k/ua.base3" as default. After that, the script will show two main class and ask you to pick one of them. The first one is going to calculate the time to train the model using ALS. The second one is only going to evaluate the model by measuring the Mean Squared Error.

##Data
The default dataset used as default is in this format:

User, Movie, Rank

It's a small file. If you wish to use one bigger then you can download it from the "http://grouplens.org/datasets/movielens/" website. However, after you do it you have to use the script format.sh which is going to format the file. For example:

 bash format.sh /home/arthur/ratings.dat 

The above command is going to take the ratings.dat file, format it and save it into the "data/" directory as "ratigns.txt". After that, you just need to pass this file location to the run script.


##Output
At the ouput, we either evaluate the recommendation by measuring the Mean Squared Error of rating prediction or we calculate the time to train the model. Both outputs are written in a file called "output.txt" that is saved into "data/" directory.

