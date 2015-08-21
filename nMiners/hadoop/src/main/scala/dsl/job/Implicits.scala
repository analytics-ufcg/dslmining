package dsl.job

object Implicits {
  type DataSet = String
  type WithName = (Producer, String)

  implicit def producer2withName(prod: Producer): PimpedProducer = new PimpedProducer(prod)

  implicit def job2PimpedJob(job: Job): PimpedJob = new PimpedJob(job)

  implicit def string2pimpedString(str: String): PimpedString = new PimpedString(str)

  /**
   * Convert implicitly a tuple of string to a tuple of Produced 
   * @param tuple tuple of string
   * @return
   */
  implicit def stringTuple2ProducedTuple(tuple: (String, String)): (Produced, Produced) = tuple match {
    case (name1, name2) =>
      val produced1 : Produced = Context.produceds.find(_.name equals name1).
        getOrElse(throw new IllegalArgumentException(s"there is no produced named $name1"))
      val produced2 : Produced = Context.produceds.find(_.name equals name2).
        getOrElse(throw new IllegalArgumentException(s"there is no produced named $name2"))
      (produced1, produced2)
  }
  
  // Convert a int to Node
  implicit def int2node(int: Int): Node = new Node(int)


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

  class Node(int: Int){
    def process = int
  }
}


