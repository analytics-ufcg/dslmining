package dsl_spark.job

import scala.collection.mutable.HashSet
import scala.collection.mutable.Queue

object Context {

  val INPUT_PATH_KEY: String = "INPUT_PATH"
  val OUTPUT_PATH_KEY: String = "OUTPUT_PATH"

  var basePath: String = ""

  val jobs = new Queue[Job]()

  val produceds = new HashSet[Produced[_]]()


  def clearQueues() = {
    jobs.clear()
    produceds.clear()
  }
}
