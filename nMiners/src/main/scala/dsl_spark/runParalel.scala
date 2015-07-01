package dsl_spark

import com.typesafe.config.ConfigFactory
import dsl.job.Implicits._
import dsl.job.JobUtils.in_parallel
import dsl.job.{WordCount, execute}

object Paralel extends App {


  val config = ConfigFactory.load()
  val dataset = config.getString("nMiners.inputTests")
  val output = "src/main/resources/wordCount"
  in_parallel(WordCount(dataset, output + "1") and WordCount(dataset, output + "2")) then
    WordCount(dataset, output + "3") then execute
}
