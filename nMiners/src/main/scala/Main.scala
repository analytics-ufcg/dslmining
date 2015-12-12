import com.typesafe.config.ConfigFactory
import spark.dsl.job._
import JobUtils._
import spark.dsl.job._
import Implicits._

object Main {

  def main(args: Array[String]): Unit = {

//    val dataset = "data/input.dat"
//    val output = "src/main/resources/output2.dat"

     //val dataset = "/home/lucas/mestrado/CODE/generate-test-data/a.csv"
     //val dataset = "/home/lucas/mestrado/CODE/dslmining/nMiners/data/b.csv"
     //val output = "/home/lucas/mestrado/CODE/generate-test-data/outwwr"
     val dataset = args(0)
     val output = args(1)
     //val masterUrl = args(2)
     //val jar = args(3)

    //Context.masterUrl -> "spark://ec2-52-33-227-29.us-west-2.compute.amazonaws.com:7077"
    //Context.masterUrl -> "local"
    //Context.jar -> jar

    parse_data on dataset then
      produce(user_vectors as "user_vector") then
      produce(similarity_matrix) then
      multiply("user_vector" by "similarity_matrix") then
      produce(recommendation) write_on output then execute
  }
}