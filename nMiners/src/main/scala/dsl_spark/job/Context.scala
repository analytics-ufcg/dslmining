package dsl_spark.job

import scala.collection.mutable.HashSet
import scala.collection.mutable.Queue

object Context {

  val INPUT_PATH_KEY: String = "INPUT_PATH"
  val OUTPUT_PATH_KEY: String = "OUTPUT_PATH"

  var basePath: String = ""

  val jobs = new Queue[Job]()

  val produceds = new HashSet[Produced[_]]()

  def producedsByType[T <: Producer[_]] : Option[T] = produceds.find { p => p.producer match {
    case producer: T => true
    case _ => false
  }}.map(_.producer.asInstanceOf[T])

  def clearQueues() = {
    jobs.clear()
    produceds.clear()
  }
}
