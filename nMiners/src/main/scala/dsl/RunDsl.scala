package dsl

import com.typesafe.config.ConfigFactory
import dsl.job.Implicits._
import dsl.job.JobUtils._
import dsl.job._

object RunDsl extends App {

  //NotificationEndServer.stop
  //NotificationEndServer.start

  //ConfigFactory load the values of main/resources/application.conf file
  val config = ConfigFactory.load()
  val dataset = config.getString("nMiners.inputTests")
  val output = config.getString("nMiners.out")

  parse_data on dataset in (5 process) then
    produce(user_vector)  then
    produce(similarity_matrix using COOCURRENCE as "coocurrence") then
    multiply("coocurrence" by "user_vector") then
    produce(recommendation) write_on output then execute

  //NotificationEndServer.stop
}