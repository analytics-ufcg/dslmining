import api.{UserBasedRecommenderImpl, train}
import dsl.Implicits._
import dsl.{USER_BASED_RECOMMENDER, PEARSON_CORRELATION}

object main {
  //Class to run an example of the DSL
  def main(args:Array[String]): Unit ={
    //Using the DSL, train on the dataset "data/intro1.csv" a user based recommender using pearson correlation
    val recommender = train on_dataset "data/intro1.csv" a USER_BASED_RECOMMENDER using PEARSON_CORRELATION neighbourhoodSize 10

    //Recomends to user 2 10 items
    println(recommender to 2l recommends 10)

  }

}
