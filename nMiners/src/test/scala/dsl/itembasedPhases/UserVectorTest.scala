package dsl.itembasedPhases

import com.typesafe.config.ConfigFactory
import dsl.job.Implicits._
import dsl.job._
import org.scalatest.{FlatSpec, Matchers}

class UserVectorTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "user_vector" should "save a user vector" in {
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
    dsl.job.JobUtils.produce(user_vector) write_on outputPath then dsl.job.execute


  }

  it should "in 2 process" in {
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
    dsl.job.JobUtils.produce(user_vector) in (2 process) write_on outputPath then dsl.job.execute

  }

  it should "associate to a variable" in {
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(user_vector as "user_vec") write_on outputPath then dsl.job.execute

  }

  it should "associate to a variable and in 2 process" in {
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(user_vector as "user_vec") in (2 process)  write_on outputPath then dsl.job.execute

  }



}
