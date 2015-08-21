package spark.dsl

//    UserVectorDriver.start()


import com.typesafe.config.ConfigFactory
import spark.dsl.job._
import spark.dsl.job.parse_data
import org.scalatest.{FlatSpec, Matchers}
import spark.dsl.job.JobUtils._
import java.io._
import spark.dsl.job.Implicits._

class MultiplyTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "multiply" should "run" in {
    val dataSet = "src/test/resources/data_1/actions.csv"

    parse_data on dataSet then
      JobUtils.produce(user_vectors) then
      job.JobUtils.produce(similarity_matrix) then
      multiply("similarity_matrix" by "user_vectors") then spark.dsl.job.execute
  }

   it should "write" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/multiply/"


     delete(new File(outputPath)) should be equals true

     parse_data on dataSet then
      job.JobUtils.produce(user_vectors) then
      job.JobUtils.produce(similarity_matrix) then
      multiply("similarity_matrix" by "user_vectors") write_on (outputPath) then job.execute

  }

  it should "associate to a variable" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/multiply/"

    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false


    parse_data on dataSet then
      job.JobUtils.produce(user_vectors as "matrix1")then
      job.JobUtils.produce(similarity_matrix as "matrix2") then
      multiply("matrix1" by "matrix2") write_on (outputPath) then job.execute


    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false

    parse_data on dataSet then
      job.JobUtils.produce(user_vectors)then
      job.JobUtils.produce(similarity_matrix as "matrix2") then
      multiply("user_vectors" by "matrix2") write_on (outputPath) then job.execute

    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false

    parse_data on dataSet then
      job.JobUtils.produce(user_vectors as "matrix1")then
      job.JobUtils.produce(similarity_matrix) then
      multiply("matrix1" by "similarity_matrix") write_on (outputPath) then job.execute

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
