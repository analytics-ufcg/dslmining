package dsl.job

import org.apache.mahout.math.drm.DrmLike

object Implicits {
  type DataSet = String
  type WithName = (Producer[_], String)

  implicit def producer2withName(prod: Producer[_]): PimpedProducer = new PimpedProducer(prod)

  implicit def job2PimpedJob(job: Job): PimpedJob = new PimpedJob(job)

  implicit def string2pimpedString(str: String): PimpedString = new PimpedString(str)

  /**
   * Convert implicitly a tuple of string to a tuple of Produced 
   * @param tuple tuple of string
   * @return
   */
  implicit def stringTuple2ProducedTuple(tuple: (String, String)): (Produced[DrmLike[Int]], Produced[DrmLike[Int]]) = tuple match {
    case (name1, name2) =>
      val produced1: Produced[DrmLike[Int]] = Context.produceds.find(_.name equals name1).
        map(_.asInstanceOf[Produced[DrmLike[Int]]]).
        getOrElse(throw new IllegalArgumentException(s"there is no produced named $name1"))
      val produced2: Produced[DrmLike[Int]] = Context.produceds.find(_.name equals name2).
        map(_.asInstanceOf[Produced[DrmLike[Int]]]).
        getOrElse(throw new IllegalArgumentException(s"there is no produced named $name2"))

      (produced1, produced2)
  }

  // Convert a int to Node
  implicit def int2node(int: Int): Node = new Node(int)


  class PimpedProducer(val prod: Producer[_]) {
    def as(name: String): WithName = (prod, name)
  }

  class PimpedJob(val job: Job) {
    def and(j: Job) = List(job, j)

    def and(l: List[Job]): List[Job] = job +: l
  }

  class PimpedString(val str: String) {
    def by(that: String) = (str, that)
  }

  class Node(int: Int) {
    def process = int
  }

}


