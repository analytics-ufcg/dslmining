package dsl.job

import java.io.File

import scala.collection.mutable
import scala.collection.mutable.Queue

object Context {

  val INPUT_PATH_KEY: String = "INPUT_PATH"
  val OUTPUT_PATH_KEY: String = "OUTPUT_PATH"

  var basePath: String = ""

  val jobs = new Queue[Job]()

  val produceds = new Queue[Produced]()
  val paths = new mutable.HashMap[String,String]()

  def getInputPath(): String = {
    paths(INPUT_PATH_KEY)
  }

  def addFinalOutput(outputPath: String) = {
    paths(OUTPUT_PATH_KEY) = outputPath
  }
  def addInputPath(inputPath: String) = {
    paths(INPUT_PATH_KEY) = inputPath
    val file = new File(inputPath)
    basePath = file.getParent
  }

  def clearQueues() = {
    jobs.clear()
    produceds.clear()
    paths.clear()
  }

}
