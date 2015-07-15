package dsl_hadoop.itembasedPhases_hadoop

import com.typesafe.config.ConfigFactory
import dsl_hadoop.job.Implicits._
import dsl_hadoop.job.JobUtils._
import dsl_hadoop.job._
import org.scalatest.{FlatSpec, Matchers}

class SimilarityMatrixTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "similarity_matrix" should "save a similarity_matrix using coocurrence" in {
    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl_hadoop.job.JobUtils.produce(user_vectors)  then
    dsl_hadoop.job.JobUtils.produce(similarity_matrix using COOCURRENCE) write_on outputPath then dsl_hadoop.job.execute


  }

  it should "save a similarity_matrix using coocurrence in 5 process" in {
    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
    dsl_hadoop.job.JobUtils.produce(similarity_matrix using COOCURRENCE) in (2 process) write_on outputPath then dsl_hadoop.job.execute

  }

  it should "save a similarity_matrix using coocurrence and associate to a variable" in {
    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl_hadoop.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "cooc") write_on outputPath then dsl_hadoop.job.execute

  }

  it should "save a similarity_matrix using coocurrence and associate to a variable and in 2 process" in {
    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl_hadoop.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "cooc") in (2 process)  write_on outputPath then dsl_hadoop.job.execute

  }



}
