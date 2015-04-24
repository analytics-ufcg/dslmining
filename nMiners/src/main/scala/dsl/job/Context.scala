package dsl.job

import scala.collection.mutable.Queue


object Context {


  val jobs = new Queue[Job]()

  val produceds = new Queue[Produced]()


  def clearQueues() = {
    jobs.clear()
    produceds.clear()
  }

}
