package dsl

import com.typesafe.config.ConfigFactory
import dsl.job.Implicits._
import dsl.job.JobUtils._
import dsl.job._

object RunDsl extends App {

  val config = ConfigFactory.load()
  val dataset = config.getString("nMiners.in")
  val output = config.getString("nMiners.out")

  parse_data on dataset number_of_nodes 5 then
    in_parallel(produce(coocurrence_matrix as "coocurrence") and
      produce(user_vector as "user_vectors")) number_of_nodes 6 then
    multiply("coocurrence" by "user_vectors") number_of_nodes 3 then
    produce(recommendation as "recs") number_of_nodes 5 write_on output then execute
}