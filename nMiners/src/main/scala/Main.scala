import com.typesafe.config.ConfigFactory
import spark.dsl.job._
import JobUtils._
import spark.dsl.job._
import Implicits._

object Main {

  def main(args: Array[String]): Unit = {

//    val dataset = "data/input.dat"
//    val output = "src/main/resources/output2.dat"

//    val dataset = "/local/data/wiki_data_10m.txt"
//     val output = "/home/andryw/OUT_TEST"
    val dataset = args(0)
    val output = args(1)

    parse_data on dataset then
      produce(user_vectors as "user_vector") then
      produce(similarity_matrix) write_on output then execute
  }
}