import dsl.job.JobUtils._
import dsl.job._
import dsl.job.Implicits._

/**
 * Created by arthur on 06/04/15.
 */
object Main {

  def main(args: Array[String]): Unit = {

    val dataset = "data/input.dat"
    val output = "data/output"

    parse_data on dataset then
      produce(user_vectors as "user_vector") then
      produce(similarity_matrix using COOCURRENCE as "coocurrence") then
      multiply("coocurrence" by "user_vector") then
      produce(recommendation) write_on output then execute
  }
}