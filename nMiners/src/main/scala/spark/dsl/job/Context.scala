package spark.dsl.job

import utils.Holder

import scala.collection.mutable.HashSet
import scala.collection.mutable.Queue
import scala.reflect.ClassTag

object Context {

  val masterUrl: String = "spark://ec2-54-213-58-151.us-west-2.compute.amazonaws.com:7077"
  //val masterUrl: String = "local[5]"
  val jar: String = "/opt/nMiners.jar"
  val INPUT_PATH_KEY: String = "INPUT_PATH"
  val OUTPUT_PATH_KEY: String = "OUTPUT_PATH"

  var basePath: String = ""

  val jobs = new Queue[Job]()

  val produceds = new HashSet[Produced[_]]()

  def producedsByType[T <: Producer[_]](implicit tag: ClassTag[T]): Option[T] = produceds.find { p => p.producer match {
    case e => (e!= null) && (tag.runtimeClass equals e.getClass)
  }
  }.map{
    Holder.logger.info("[REC PBT] Started")
    _.producer.asInstanceOf[T]
  }

  def producedsByName(name: String) = produceds.find {
    _.name equals name
  }

  def clearQueues() = {
    jobs.clear()
    produceds.clear()
  }
}
