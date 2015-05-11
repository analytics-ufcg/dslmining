package dsl.itembasedPhases

import com.typesafe.config.ConfigFactory
import dsl.job.Implicits._
import dsl.job.JobUtils._
import dsl.job._
import org.scalatest.{FlatSpec, Matchers}

class SimilarityMatrixTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "similarity_matrix" should "save a similarity_matrix using coocurrence" in {
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(user_vectors)  then
    dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE) write_on outputPath then dsl.job.execute


  }

  it should "save a similarity_matrix using coocurrence in 5 process" in {
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
    dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE) in (2 process) write_on outputPath then dsl.job.execute

  }

  it should "save a similarity_matrix using coocurrence and associate to a variable" in {
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "cooc") write_on outputPath then dsl.job.execute

  }

  it should "save a similarity_matrix using coocurrence and associate to a variable and in 2 process" in {
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "cooc") in (2 process)  write_on outputPath then dsl.job.execute

  }



}
