package dsl.itembasedPhases

import dsl.job.JobUtils._
import dsl.job.{recommendation, similarity_matrix, user_vector, parse_data}
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by arthur on 06/05/15.
 */
class MultiplyTest  extends FlatSpec with Matchers{



  it should "multiply user vector by similarity matrix" in{
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      produce(user_vector)  then
      produce(similarity_matrix using COOCURRENCE as "coocurrence") then
      multiply("coocurrence" by "user_vector")  then
      produce(recommendation) write_on output then execute

  }

}
