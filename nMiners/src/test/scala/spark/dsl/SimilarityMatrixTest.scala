package spark.dsl

//    UserVectorDriver.start()


import java.io.File

import com.typesafe.config.ConfigFactory
import spark.dsl.job
import spark.dsl.job._
import spark.dsl.job.Implicits._
import spark.dsl.job.parse_data
import org.scalatest.{FlatSpec, Matchers}

class SimilarityMatrixTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "similarity_matrix" should "run" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/output_sim/"

    parse_data on dataSet then
      JobUtils.produce(user_vectors) then
      job.JobUtils.produce(similarity_matrix) then job.execute
  }

  it should "write" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/output_sim/"

    parse_data on dataSet then
      job.JobUtils.produce(user_vectors) then
      job.JobUtils.produce(similarity_matrix) write_on outputPath then job.execute

    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false

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
    val outputPath: String = "src/test/resources/DSL_Tests/output_sim/"

    parse_data on dataSet then
      job.JobUtils.produce(user_vectors as "matrix1") then
      job.JobUtils.produce(similarity_matrix as "matrix2") write_on outputPath then job.execute

    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false

  }

  it should "associate to a variable and in 2 process" in {
//    val dataSet = "src/test/resources/data_2/input_test_user_vector.txt"
//    val outputPath: String = "src/test/resources/SimplePhasesTest/output_sim/"
//
//    parse_data on dataSet then
//      dsl.job.JobUtils.produce(user_vectors as "user_vec") in (2 process)  write_on outputPath then dsl.job.execute

  }

  def delete(file: File):Boolean ={
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
    return file.delete
  }

  def fileExists(file: File): Boolean ={
    file.exists()
  }



}
