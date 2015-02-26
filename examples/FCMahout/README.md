##FCMahout 
Implementation from the example for the fifth chapter for mahout in action

For this example we created one User-based recommender from a dating site. 
we used similarity PearsonCorrelation and neighborhood NearestNUserNeighborhood equals to 10  

##Data 

You should download the dataset from http://www.occamslab.com/petricek/data/libimseti-complete.zip
Extract the zip in the folder data and rename the file ratings.dat to libimset-ratings.dat, and the file gender.dat to libimset-gender.dat. 
Copy libimset-ratings.dat and libimset-gender.dat to the folder data

##Execution
To run the program you just need to use the scripts named activator on the root of this project,
using the parameter run.

example:

./activator run

It might take some time the first time you run because it will download all the dependencies nedded.

##Output

We used RMSRecommender evaluator. To compare we used two differents recommenderBuilder,
one random and genericUserBased. 
This way it is possible see that genericUserBased it is better than random. 

