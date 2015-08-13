package dsl_spark

//    UserVectorDriver.start()


import java.io.File

import com.typesafe.config.ConfigFactory
import dsl_spark.job.Implicits._
import dsl_spark.job.{parse_data, user_vectors}
import org.scalatest.{FlatSpec, Matchers}

class UserVectorTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  val config = ConfigFactory.load()

  "user_vector" should "run" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/users_vectors/"

    parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors) then dsl_spark.job.execute
  }

  "user_vector" should "write" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/users_vectors/"

    parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors) write_on outputPath then dsl_spark.job.execute

    fileExists(new File(outputPath)) should be equals true
    delete(new File(outputPath)) should be equals true
    fileExists(new File(outputPath)) should be equals false

  }


  it should "associate to a variable" in {
    val dataSet = "src/test/resources/data_1/actions.csv"
    val outputPath: String = "src/test/resources/DSL_Tests/users_vectors/"

    parse_data on dataSet then
      dsl_spark.job.JobUtils.produce(user_vectors as "matrix") write_on outputPath then dsl_spark.job.execute

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
