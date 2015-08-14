import com.typesafe.config.ConfigFactory
import spark.dsl.job._
import JobUtils._
import spark.dsl.job._
import Implicits._

object Main {

  def main(args: Array[String]): Unit = {
    
    val dataset = "data/input.dat"
    val output = "src/main/resources/output2.dat"

    parse_data on dataset then
      produce(user_vectors as "user_vector") then
      produce(similarity_matrix) then
      multiply("user_vector" by "similarity_matrix") then
      produce(recommendation) write_on output then execute
  }
}