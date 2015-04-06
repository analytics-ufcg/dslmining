import api.{UserBasedRecommenderImpl, train}
import dsl.Implicits._
import dsl.{MATRIX_FAC_RECOMMENDER, USER_BASED_RECOMMENDER, PEARSON_CORRELATION}

object main {
  //Class to run an example of the DSL
  def main(args:Array[String]): Unit ={
    //Using the DSL, train on the dataset "data/intro1.csv" a user based recommender using pearson correlation
    println("========================================Training UserBased Recommender on data/intro1.csv data")
    val recommender = train on_dataset "data/intro1.csv" a USER_BASED_RECOMMENDER using PEARSON_CORRELATION neighbourhoodSize 10

        //Recomends to user 2 10 items
    println("========================================Recommending to user 2")
    println(recommender to 2 recommends 10)
    println(recommender to 2 recommends 10)

    //println(train on_dataset "data/intro1.csv" a USER_BASED_RECOMMENDER using PEARSON_CORRELATION neighbourhoodSize 10)
  }

}
