package dsl.itembasedPhases

import dsl.job._
import org.scalatest.{Matchers, FlatSpec}
import dsl.job.Implicits._
import dsl.job.JobUtils._
import java.io._


import scala.reflect.io.Path


/**
 * Created by arthur on 06/05/15.
 */

class MultiplyTest  extends FlatSpec with Matchers{



  it should "multiply user vector by similarity matrix using write_on command" in{
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(user_vectors) then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "coocurrence") then
      multiply("coocurrence" by "user_vector")  write_on outputPath then dsl.job.execute

    (new File(outputPath + "data_multiplied").exists()) should be equals(true)
    Path(outputPath).deleteRecursively()
    (new File(outputPath).exists()) should be equals(false)
  }

  it should "multiply user vector by similarity matrix by using as command" in{
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    the[IllegalArgumentException] thrownBy {
      parse_data on dataSet then
        dsl.job.JobUtils.produce(user_vectors) then
        dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "matrix_coocurrence") then
        multiply("wrong_matrix" by "user_vector") then dsl.job.execute
    } should have message "there is no produced named wrong_matrix"


    parse_data on dataSet then
      dsl.job.JobUtils.produce(user_vectors) then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "matrix_coocurrence") then
      multiply("matrix_coocurrence" by "user_vector") write_on outputPath then dsl.job.execute

    (new File(outputPath + "data_multiplied").exists()) should be equals(true)
    Path(outputPath).deleteRecursively()
    (new File(outputPath).exists()) should be equals false


    parse_data on dataSet then
      dsl.job.JobUtils.produce(user_vectors as "vector") then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "matrix_coocurrence") then
      multiply("matrix_coocurrence" by "vector") then dsl.job.execute

    (new File(outputPath + "data_multiplied").exists()) should be equals(false)
  }


  it should "get the same matrix in both cases" in{
    val dataSet = "src/test/resources/data_2/input_test_level1.txt"
    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"

    parse_data on dataSet then
      dsl.job.JobUtils.produce(user_vectors as "vector") then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "matrix_coocurrence") then
      multiply("matrix_coocurrence" by "vector") then dsl.job.execute


    the[Exception] thrownBy {
      parse_data on dataSet then
        dsl.job.JobUtils.produce(user_vectors as "vector") then
        dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "matrix_coocurrence") then
        multiply("vector" by "matrix_coocurrence") write_on outputPath then dsl.job.execute
    } should have message "Matrix one's columns and Matrix two's lines are not equal"
  }

}

