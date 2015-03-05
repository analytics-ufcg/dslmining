## PythonSpark

It is a implementation of a recommender from the spark website using Python.

## Data 

In order to run the recommender we used a dataset from the GroupLens website. The dataset is saved into the file called ua.base. This file has 100.000 ratings from 1000 users on 1700 movies.

## Execution
In oder to execute the script you have to run the ./bin/pyspark inside the spark project and, also,
give the python script location.

example:

./bin/pyspark PythonCollaborative.py

## Output
At the ouput, we evaluate the recommendation by measuring the Mean Squared Error of rating prediction.

