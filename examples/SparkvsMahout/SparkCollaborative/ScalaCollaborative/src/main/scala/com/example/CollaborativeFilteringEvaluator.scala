package com.example


import java.io.FileWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object CollaborativeFilteringEvaluator {

  def main(args: Array[String]): Unit = {
    // Load and parse the data
    val conf = new SparkConf().setAppName("Scala Collaborative").setMaster("local")
    val sc = new SparkContext(conf)

    val data = if(args.isEmpty) sc.textFile("data/ml-100k/ua.base3")  else sc.textFile(args(0))

    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 20

      val model = ALS.train(ratings, rank, numIterations, 0.01)

    val fw = new FileWriter("data/output.txt", false) ;

    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    fw.write("Mean Squared Error = " + MSE + "\n")
    fw.close()
  }
}



