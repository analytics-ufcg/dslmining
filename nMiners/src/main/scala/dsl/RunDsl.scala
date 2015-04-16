package dsl

import com.typesafe.config.ConfigFactory
import dsl.job.Implicits._
import dsl.job.JobUtils._
import dsl.job._

object RunDsl extends App {

  val config = ConfigFactory.load()
  val dataset = config.getString("nMiners.in")
  val output = config.getString("nMiners.out")

  parse_data on dataset in 5.proccess then
    in_parallel(produce(coocurrence_matrix as "coocurrence") and
      produce(user_vector as "user_vectors")) in 6.proccess then
    multiply("coocurrence" by "user_vectors") in 3.proccess then
    produce(recommendation as "recs") in 5.proccess write_on output then execute
}