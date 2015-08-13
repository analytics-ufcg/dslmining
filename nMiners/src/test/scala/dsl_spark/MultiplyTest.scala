package dsl_spark

//    UserVectorDriver.start()


import com.typesafe.config.ConfigFactory
import dsl_spark.job.{parse_data, similarity_matrix, user_vectors}
import org.scalatest.{FlatSpec, Matchers}
import dsl_spark.job.JobUtils._
import java.io._
import dsl_spark.job.Implicits._

class MultiplyTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "multiply" should "run" in {
    val dataSet = "src/test/resources/data_1/actions.csv"

    parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors) then
      dsl_spark.job.JobUtils.produce(similarity_matrix) then
      multiply("similarity_matrix" by "user_vectors") then dsl_spark.job.execute
  }

   it should "write" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/multiply/"


     delete(new File(outputPath)) should be equals true

     parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors) then
      dsl_spark.job.JobUtils.produce(similarity_matrix) then
      multiply("similarity_matrix" by "user_vectors") write_on (outputPath) then dsl_spark.job.execute

  }

  it should "associate to a variable" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/multiply/"

    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false


    parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors as "matrix1")then
      dsl_spark.job.JobUtils.produce(similarity_matrix as "matrix2") then
      multiply("matrix1" by "matrix2") write_on (outputPath) then dsl_spark.job.execute


    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false

    parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors)then
      dsl_spark.job.JobUtils.produce(similarity_matrix as "matrix2") then
      multiply("user_vectors" by "matrix2") write_on (outputPath) then dsl_spark.job.execute

    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false

    parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors as "matrix1")then
      dsl_spark.job.JobUtils.produce(similarity_matrix) then
      multiply("matrix1" by "similarity_matrix") write_on (outputPath) then dsl_spark.job.execute

    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false
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
