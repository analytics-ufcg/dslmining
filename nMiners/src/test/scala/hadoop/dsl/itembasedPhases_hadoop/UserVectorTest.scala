package hadoop.dsl.itembasedPhases_hadoop

import com.typesafe.config.ConfigFactory
import hadoop.dsl.job.Implicits._
import hadoop._
import hadoop.dsl.job.{user_vectors, parse_data}
import org.scalatest.{FlatSpec, Matchers}

class UserVectorTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "user_vector" should "save a user vector" in {
    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
    dsl.job.JobUtils.produce(user_vectors) write_on outputPath then dsl.job.execute


  }

  it should "in 2 process" in {
    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
    dsl.job.JobUtils.produce(user_vectors) in (2 process) write_on outputPath then dsl.job.execute

  }

  it should "associate to a variable" in {
    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(user_vectors as "user_vec") write_on outputPath then dsl.job.execute

  }

  it should "associate to a variable and in 2 process" in {
    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(user_vectors as "user_vec") in (2 process)  write_on outputPath then dsl.job.execute

  }



}
