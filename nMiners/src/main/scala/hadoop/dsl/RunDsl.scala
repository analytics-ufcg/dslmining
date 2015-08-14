package hadoop.dsl

import com.typesafe.config.ConfigFactory
import hadoop.dsl.job.Implicits._
import hadoop.dsl.job.JobUtils._
import hadoop.dsl.job._

object RunDsl extends App {

  //ConfigFactory load the values of main/resources/application.conf file
  val config = ConfigFactory.load()
  val dataset = "src/main/resources/input.dat"
  val output = "src/main/resources/output.dat"

  parse_data on dataset then
    produce(user_vectors)  then
    produce(similarity_matrix using COOCURRENCE as "coocurrence") then
    multiply("coocurrence" by "user_vectors") then
    produce(recommendation) write_on output then execute
}