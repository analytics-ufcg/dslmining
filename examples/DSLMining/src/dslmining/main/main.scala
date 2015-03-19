import api.{UserBasedRecommenderImpl, train}
import dsl.Implicits._
import dsl.{USER_BASED_RECOMMENDER, PEARSON_CORRELATION}

/**
 * Created by andryw on 17/03/15.
 */
object main {
  def main(args:Array[String]): Unit ={
    val recommender:UserBasedRecommenderImpl = train on_dataset "data/intro1.csv" a USER_BASED_RECOMMENDER using PEARSON_CORRELATION neighbourhoodSize 10

    println(recommender to 2l recommends 10)

  }

}
