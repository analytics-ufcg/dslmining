package dsl

import com.typesafe.config.ConfigFactory
import dsl.job.Implicits._
import dsl.job.JobUtils._
import dsl.job._

object RunDsl extends App {

  //ConfigFactory load the values of main/resources/application.conf file
  val config = ConfigFactory.load()
  val dataset = config.getString("nMiners.in")
  val output = config.getString("nMiners.out")

  parse_data on dataset then
    produce(user_vectors)  then
    produce(similarity_matrix using COOCURRENCE as "coocurrence") then
    multiply("coocurrence" by "user_vectors") then
    produce(recommendation) write_on output then execute
}