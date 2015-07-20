package dsl_hadoop.itembasedPhases

//    UserVectorDriver.start()


import com.typesafe.config.ConfigFactory
import dsl_spark.job.{parse_data, similarity_matrix, user_vectors}
import org.scalatest.{FlatSpec, Matchers}
import dsl_spark.job.JobUtils._
import dsl_spark.job.Implicits._

class MultiplyTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "multiply" should "run" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/multiply/"

    parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors) then
      dsl_spark.job.JobUtils.produce(similarity_matrix) then
      multiply("similarity_matrix" by "user_vectors") then dsl_spark.job.execute
  }

  "multiply" should "write" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/multiply/"

    parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors) then
      dsl_spark.job.JobUtils.produce(similarity_matrix) then
      multiply("similarity_matrix" by "user_vectors") write_on (outputPath) then dsl_spark.job.execute


  }

  it should "in 2 process" in {
//    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
//    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"
//
//    parse_data on dataSet then
//    dsl.job.JobUtils.produce(user_vectors) in (2 process) write_on outputPath then dsl.job.execute

  }

  it should "associate to a variable" in {
//    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
//    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"
//
//    parse_data on dataSet then
//      dsl.job.JobUtils.produce(user_vectors as "user_vec") write_on outputPath then dsl.job.execute

  }

  it should "associate to a variable and in 2 process" in {
//    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
//    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"
//
//    parse_data on dataSet then
//      dsl.job.JobUtils.produce(user_vectors as "user_vec") in (2 process)  write_on outputPath then dsl.job.execute

  }



}
