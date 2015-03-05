package com.example


import java.io.FileWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object CollaborativeFiltering {

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

    var time = System.currentTimeMillis;
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    time =  System.currentTimeMillis - time

    val fw = new FileWriter("data/output.txt", false) ;

    fw.write("Execution time for training: " + time + "\n")
    fw.close()
  }
}



