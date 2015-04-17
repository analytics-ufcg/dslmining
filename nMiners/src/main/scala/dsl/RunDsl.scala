package dsl

import com.typesafe.config.ConfigFactory
import dsl.job.Implicits._
import dsl.job.JobUtils._
import dsl.job._

object RunDsl extends App {

  val config = ConfigFactory.load()
  val dataset = config.getString("nMiners.in")
  val output = config.getString("nMiners.out")

  parse_data on dataset then
    in_parallel(produce(coocurrence_matrix) and
      produce(user_vector)) then
    multiply("coocurrence" by "user_vectors") then
    produce(recommendation) write_on output then execute
}