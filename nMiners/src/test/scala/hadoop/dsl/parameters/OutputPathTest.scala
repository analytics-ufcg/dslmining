package hadoop.dsl.parameters

import java.nio.file.{Files, Paths}

import hadoop.dsl.job.JobUtils._
import hadoop.dsl.job._
import hadoop._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import hadoop.dsl.job.Implicits._

import scala.collection.mutable
import scala.reflect.io.Path

class OutputPathTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  val BASE_PATH = "src/test/resources/data_2/"
  val BASE_OUTPUT_PATH = "src/test/resources/ParseDataTest/"
  val INPUT_1 = BASE_PATH + "input_test_user_vector.txt"
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

    Files.exists(Paths.get(OUTPUTS("OUTPUT_1") )) shouldBe false

    parse_data on dataset then
      dsl.job.JobUtils.produce(user_vectors)  then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE) then
      multiply("similarity_matrix" by "user_vectors") then
      dsl.job.JobUtils.produce(recommendation) write_on output then dsl.job.execute


    Files.exists(Paths.get(OUTPUTS("OUTPUT_1") )) shouldBe true
    Files.exists(Paths.get(output)) shouldBe true
    Files.exists(Paths get Context.paths("user_vectors")) shouldBe true
    Files.exists(Paths get Context.paths("similarity_matrix")) shouldBe true

    Path(OUTPUTS("OUTPUT_1")) deleteRecursively()
    Path(Context.paths("user_vectors")) deleteRecursively()
    Path(Context.paths("similarity_matrix")) deleteRecursively()


  }

  "rundsl with as" should "run" in {
    val dataset = INPUT_1
    val output = OUTPUTS("OUTPUT_2") + "output.dat"

    Files.exists(Paths.get(OUTPUTS("OUTPUT_2") )) shouldBe false

    println(BASE_PATH)
    println(OUTPUTS("OUTPUT_2"))
    println(OUTPUTS("OUTPUT_3"))


    parse_data on dataset then
      dsl.job.JobUtils.produce(user_vectors as "user_v")  then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "coocurrence") then
      multiply("coocurrence" by "user_v") then
      dsl.job.JobUtils.produce(recommendation) write_on output then dsl.job.execute

    Files.exists(Paths.get(OUTPUTS("OUTPUT_2") )) shouldBe true
    Files.exists(Paths.get(output)) shouldBe true
    Files.exists(Paths get Context.paths("user_v")) shouldBe true
    Files.exists(Paths get Context.paths("coocurrence")) shouldBe true

    Path(OUTPUTS("OUTPUT_2")) deleteRecursively()
    Path(Context.paths("user_v")) deleteRecursively()
    Path(Context.paths("coocurrence")) deleteRecursively()


  }

  "rundsl with write" should "run" in {
    val dataset = INPUT_1
    val output = OUTPUTS("OUTPUT_3") + "output.dat"

    Files.exists(Paths.get(OUTPUTS("OUTPUT_3") )) shouldBe false


    parse_data on dataset then
      dsl.job.JobUtils.produce(user_vectors) write_on (OUTPUTS("OUTPUT_3") + "/userv")  then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "coocurrence") write_on (OUTPUTS("OUTPUT_3") + "/cooc")then
      multiply("coocurrence" by "user_vectors") write_on (OUTPUTS("OUTPUT_3") + "/matrix") then
      dsl.job.JobUtils.produce(recommendation) write_on output then dsl.job.execute

    Files.exists(Paths.get(OUTPUTS("OUTPUT_3") )) shouldBe true
    Files.exists(Paths.get(OUTPUTS("OUTPUT_3") + "/userv")) shouldBe true
    Files.exists(Paths.get(OUTPUTS("OUTPUT_3") + "/cooc" )) shouldBe true
    Files.exists(Paths.get(OUTPUTS("OUTPUT_3") + "/matrix" )) shouldBe true

    Files.exists(Paths.get(output)) shouldBe true

    Path(OUTPUTS("OUTPUT_3")) deleteRecursively()


  }

  "rundsl with write and as" should "run" in {
    val dataset = INPUT_1
    val output = OUTPUTS("OUTPUT_4") + "output.dat"

    Files.exists(Paths.get(output )) shouldBe false


    parse_data on dataset then  dsl.job.JobUtils.produce(user_vectors as "user_v")  write_on (OUTPUTS("OUTPUT_4") + "/userv") then
      dsl.job.JobUtils.produce(similarity_matrix using COOCURRENCE as "coocurrence") write_on (OUTPUTS("OUTPUT_4") + "/cooc") then
      multiply("coocurrence" by "user_v")  write_on (OUTPUTS("OUTPUT_4") + "/matrix") then
      dsl.job.JobUtils.produce(recommendation) write_on output then dsl.job.execute



    Files.exists(Paths.get(OUTPUTS("OUTPUT_4") )) shouldBe true
    Files.exists(Paths.get(OUTPUTS("OUTPUT_4") + "/userv")) shouldBe true
    Files.exists(Paths.get(OUTPUTS("OUTPUT_4") + "/cooc" )) shouldBe true
    Files.exists(Paths.get(OUTPUTS("OUTPUT_4") + "/matrix" )) shouldBe true

    Files.exists(Paths.get(output)) shouldBe true

    Path(OUTPUTS("OUTPUT_4")) deleteRecursively()


  }



}
