package dsl.itembasedPhases

import dsl.job.Implicits._
import dsl.job.JobUtils._
import dsl.job._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable
import scala.reflect.io.Path

class OverallTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  val BASE_PATH = "src/test/resources/data_2/"
  val BASE_OUTPUT_PATH = "src/test/resources/ParseDataTest/"
  val INPUT_1 = BASE_PATH + "input_test_level1.txt"
  val OUTPUTS = new mutable.HashMap[String,String]()
  OUTPUTS("OUTPUT_1") =  BASE_OUTPUT_PATH + "save_output1/"
  OUTPUTS("OUTPUT_2") =  BASE_OUTPUT_PATH + "save_output2/"
  OUTPUTS("OUTPUT_3") =  BASE_OUTPUT_PATH + "save_output3/"
  OUTPUTS("OUTPUT_4") =  BASE_OUTPUT_PATH + "save_output4/"

  override def beforeAll(): Unit ={
    OUTPUTS foreach{case (key, value)  => {
      val path: Path = Path (value)
      path deleteRecursively
    }}
  }
  "rundsl" should "run" in {
    val dataset = INPUT_1
    val output = OUTPUTS("OUTPUT_1") + "output.dat"

    parse_data on dataset then
      dsl.job.JobUtils.produce(user_vector)  then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE) then
      multiply("similarity_matrix" by "user_vector") then
      dsl.job.JobUtils.produce(recommendation) write_on output then dsl.job.execute
  }

  "rundsl with as" should "run" in {
    val dataset = INPUT_1
    val output = OUTPUTS("OUTPUT_2") + "output.dat"
    parse_data on dataset then
      dsl.job.JobUtils.produce(user_vector as "user_v")  then
    dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "coocurrence") then
      multiply("coocurrence" by "user_v") then
      dsl.job.JobUtils.produce(recommendation) write_on output then dsl.job.execute

  }

  "rundsl with write" should "run" in {
    val dataset = INPUT_1
    val output = OUTPUTS("OUTPUT_3") + "output.dat"
    parse_data on dataset then
      dsl.job.JobUtils.produce(user_vector) write_on (OUTPUTS("OUTPUT_3") + "/userv")  then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "coocurrence") write_on (OUTPUTS("OUTPUT_3") + "/cooc")then
      multiply("coocurrence" by "user_vector") write_on (OUTPUTS("OUTPUT_3") + "/matrix") then
      dsl.job.JobUtils.produce(recommendation) write_on output then dsl.job.execute

  }

  "rundsl with write and as" should "run" in {
    val dataset = INPUT_1
    val output = OUTPUTS("OUTPUT_4") + "output.dat"
    parse_data on dataset then  dsl.job.JobUtils.produce(user_vector as "user_v")  write_on (OUTPUTS("OUTPUT_4") + "/userv") then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "coocurrence") write_on (OUTPUTS("OUTPUT_4") + "/cooc") then
      multiply("coocurrence" by "user_v")  write_on (OUTPUTS("OUTPUT_4") + "/matrix") then
      dsl.job.JobUtils.produce(recommendation) write_on output then dsl.job.execute

  }



}
