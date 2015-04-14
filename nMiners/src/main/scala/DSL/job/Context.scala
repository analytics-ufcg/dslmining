package DSL.job

import scala.collection.mutable.Queue


object Context {

  val jobs = new Queue[Job]()

  val produceds = new Queue[Produced]()
}
