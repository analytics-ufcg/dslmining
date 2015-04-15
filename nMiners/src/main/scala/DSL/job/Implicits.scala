package DSL.job

object Implicits {
  type DataSet = String
  type WithName = (Producer, String)

  implicit def producer2withName(prod: Producer): PimpedProducer = new PimpedProducer(prod)

  implicit def job2PimpedJob(job: Job): PimpedJob = new PimpedJob(job)

  implicit def string2pimpedString(str: String) = new PimpedString(str)

  // Find produced
  implicit def stringTuple2ProducedTuple(tuple: (String, String)): (Produced, Produced) = tuple match {
    case (name1, name2) => {
      val produced1 : Produced = Context.produceds.find(_.name equals name1).
        getOrElse(throw new IllegalArgumentException(s"there is no produced named $name1"))
      val produced2 : Produced = Context.produceds.find(_.name equals name2).
        getOrElse(throw new IllegalArgumentException(s"there is no produced named $name2"))
      (produced1, produced2)
    }
  }

  implicit def int2node(int: Int) = new Node(int)

//  class Pimped(val int: Int){
//    def nodes() = int
//  }

  class PimpedProducer(val prod: Producer) {
    def as(name: String): WithName = (prod, name)
  }

  class PimpedJob(val job: Job) {
    def and(j: Job) = List(job, j)
    def and(l: List[Job]) :List[Job]= job +: l
  }

  class PimpedString(val str: String) {
    def by(that: String) = (str, that)
  }
}

class Node(int: Int){
  def nodes = int
}
