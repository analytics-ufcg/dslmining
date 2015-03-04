##ScalaSpark

   It is a implementation of a recommender from the spark website using Scala.

##Data 

In order to run the recommender we used a dataset from the GroupLens website. The dataset is saved into the file called ua.base. This file has 100.000 ratings from 1000 users on 1700 movies.

##Execution
In oder to execute the script you have to run the ./activator run inside the spark project.
example:
	./activator run

##Output
At the ouput, we evaluate the recommendation by measuring the Mean Squared Error of rating prediction.

