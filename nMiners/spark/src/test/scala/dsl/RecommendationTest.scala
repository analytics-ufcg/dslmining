package dsl

//    UserVectorDriver.start()


import java.io.File

import com.typesafe.config.ConfigFactory
import dsl.job._
import dsl.job.Implicits._
import dsl.job.JobUtils._
import dsl.job.parse_data
import org.scalatest.{FlatSpec, Matchers}

class RecommendationTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  it should "run" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/recommender/"

    parse_data on dataSet then
      JobUtils.produce(user_vectors) then
      job.JobUtils.produce(similarity_matrix) then
      multiply("similarity_matrix" by "user_vectors") then
      job.JobUtils.produce(recommendation) then job.execute


  }

  it should "write" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/recommender/"

    parse_data on dataSet then
      job.JobUtils.produce(user_vectors) then
      job.JobUtils.produce(similarity_matrix) then
      multiply("similarity_matrix" by "user_vectors") then
      job.JobUtils.produce(recommendation) write_on(outputPath) then job.execute

  }

  it should "in 2 process" in {
//    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
//    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"
//
//    parse_data on dataSet then
//    dsl.job.JobUtils.produce(user_vectors) in (2 process) write_on outputPath then dsl.job.execute

  }

  it should "associate to a variable" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/recommender/"

    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false

    parse_data on dataSet then
      job.JobUtils.produce(user_vectors as "matrix1") then
      job.JobUtils.produce(similarity_matrix as "matrix2") then
      multiply("matrix1" by "matrix2") then
      job.JobUtils.produce(recommendation) write_on(outputPath) then job.execute
  }

  it should "associate to a variable and in 2 process" in {
//    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
//    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"
//
//    parse_data on dataSet then
//      dsl.job.JobUtils.produce(user_vectors as "user_vec") in (2 process)  write_on outputPath then dsl.job.execute

  }

  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
    file.delete
  }

  def fileExists(file: File): Boolean ={
    file.exists()
  }

}
