package dsl.itembasedPhases_hadoop

import com.typesafe.config.ConfigFactory
import dsl.job.{parse_data, _}
import org.scalatest.{FlatSpec, Matchers}


class SimplePhasesTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "parse_data" should "store an input file" in {
    val dataSet = "src/test/resources/input.dat"
    parse_data on dataSet

    parse_data.path should not be (None)
    parse_data.path should not be (empty)
    parse_data.path shouldEqual "src/test/resources/input.dat"

  }

  "produce similarity_matrix" should "save in output file" in {
    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE) write_on outputPath then dsl.job.execute

  }


}
