package dsl.itembasedPhases

import dsl.job._
import org.scalatest.{Matchers, FlatSpec}
import dsl.job.Implicits._
import dsl.job.JobUtils._


/**
 * Created by arthur on 06/05/15.
 */
class MultiplyTest  extends FlatSpec with Matchers{



  it should "multiply user vector by similarity matrix" in{
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(user_vector) then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "coocurrence") then
      multiply("coocurrence" by "user_vector")  then
      dsl.job.JobUtils.produce(recommendation) write_on outputPath then dsl.job.execute

  }

}
