import com.typesafe.config.ConfigFactory
import dsl_spark.job.JobUtils._
import dsl_spark.job._
import dsl_spark.job.Implicits._

object Main {

  def main(args: Array[String]): Unit = {

    val dataset = "data/input.dat"
    val output = "data/output"
val config = ConfigFactory.load()

//    val dataset = config.getString("nMiners.in")
//    val output = config.getString("nMiners.out")

    parse_data on dataset then
      produce(user_vectors as "user_vector") then
      produce(similarity_matrix)  then
      multiply("similarity_matrix" by "user_vector") then
      produce(recommendation) write_on output then execute
  }
}