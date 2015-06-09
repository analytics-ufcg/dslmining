package dsl.itembasedPhases


import com.typesafe.config.ConfigFactory
import dsl.job._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import dsl.job.Implicits._

import scala.collection.mutable
import scala.reflect.io.Path

class ParseDataTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  val BASE_PATH = "src/test/resources/data_2/"
  val BASE_OUTPUT_PATH = "src/test/resources/ParseDataTest/"
  val INPUT_1 = BASE_PATH + "input_test_user_vector.txt"
  val OUTPUTS = new mutable.HashMap[String,String]()
  OUTPUTS("OUTPUT_1") =  BASE_OUTPUT_PATH + "save_output/"
  OUTPUTS("OUTPUT_2") =  BASE_OUTPUT_PATH + "save_output_2_process/"

  override def beforeAll(): Unit ={
    OUTPUTS foreach{case (key, value)  => {
      val path: Path = Path (value)
      path deleteRecursively
    }}
  }
  "parse_data" should "store an input file" in {
    val dataset: String = INPUT_1
    parse_data on dataset

    parse_data.path should not be (None)
    parse_data.path should not be (empty)
    parse_data.path shouldEqual INPUT_1

  }

  it should "save in an output file" in {
    val dataset = INPUT_1
    val outputpath =  OUTPUTS("OUTPUT_1")
    parse_data on dataset write_on outputpath then dsl.job.execute


  }


  it should "store an input file in many process" in {
    val dataSet = INPUT_1
    val outputpath  = OUTPUTS("OUTPUT_2")

    parse_data on dataSet in (2 process) write_on outputpath then dsl.job.execute

  }


}
