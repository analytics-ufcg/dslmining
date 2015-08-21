package hadoop.dsl.parameters

/**
 * Created by tales on 30/04/15.
 */

import com.typesafe.config.ConfigFactory
import hadoop.dsl.job.Implicits._
import hadoop.dsl.job.JobUtils._
import hadoop.dsl.job.WordCount
import org.scalatest._
import hadoop._

import scala.io.Source.fromFile
import scala.reflect.io.Path

class InParallelJobTest extends FlatSpec with Matchers {

  val config = ConfigFactory.load()
  val dataset = config.getString("nMiners.inputTests")
  val output = "src/main/resources/wordCount"


  "Equals jobs in parallel" should "have the same output" in {

    in_parallel(WordCount(dataset, output + "1") and WordCount(dataset, output + "2")) then dsl.job.execute

    val outputs = 1 to 2 map {
      "src/main/resources/wordCount" + _ + "/part-r-00000"
    } map {
      fromFile
    } map {
      _.mkString
    }

    outputs(0) should be equals outputs(1)
  }

  it should "have the same output than sequencially" in {


    in_parallel(WordCount(dataset, output + "1") and WordCount(dataset, output + "2")) then
      WordCount(dataset, output + "3") then
      dsl.job.execute

    val outputs = 1 to 3 map {
      "src/main/resources/wordCount" + _ + "/part-r-00000"
    } map {
      fromFile
    } map {
      _.mkString
    }

    outputs(1) should be equals outputs(2)
  }
}