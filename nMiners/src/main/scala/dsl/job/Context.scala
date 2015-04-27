package dsl.job

import scala.collection.mutable
import scala.collection.mutable.Queue


object Context {
  val INPUT_PATH_KEY: String = "INPUT_PATH"

  def getInputPath(): String = {
    paths(INPUT_PATH_KEY)
  }

  def addInputPath(inputPath: String) = {
    paths(INPUT_PATH_KEY) = inputPath
  }


  val jobs = new Queue[Job]()

  val produceds = new Queue[Produced]()
  val paths = new mutable.HashMap[String,String]()


  def clearQueues() = {
    jobs.clear()
    produceds.clear()
    paths.clear()
  }

}
