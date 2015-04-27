package dsl.job

import java.io.File

import scala.collection.mutable
import scala.collection.mutable.Queue

object Context {
  val INPUT_PATH_KEY: String = "INPUT_PATH"
  var basePath: String = ""

  def getInputPath(): String = {
    paths(INPUT_PATH_KEY)
  }

  def addInputPath(inputPath: String) = {
    paths(INPUT_PATH_KEY) = inputPath
    val file = new File(inputPath)
    basePath = file.getParent
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
