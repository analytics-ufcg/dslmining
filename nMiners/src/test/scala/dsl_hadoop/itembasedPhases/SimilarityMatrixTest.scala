package dsl_hadoop.itembasedPhases

//    UserVectorDriver.start()


import com.typesafe.config.ConfigFactory
import dsl_spark.job.{parse_data, user_vectors,similarity_matrix}
import org.scalatest.{FlatSpec, Matchers}

class SimilarityMatrixTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "similarity_matrix" should "run" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/output_sim/"

    parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors) then
      dsl_spark.job.JobUtils.produce(similarity_matrix) write_on outputPath then dsl_spark.job.execute
  }

  "user_vector" should "save a user vector" in {
//    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
//    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"
//
//    parse_data on dataSet then
//    dsl.job.JobUtils.produce(user_vectors) write_on outputPath then dsl.job.execute


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
