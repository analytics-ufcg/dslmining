package DSL.job

object Implicits {
  type Dataset = String
  type WithName = (Producer, String)

  implicit def producer2withName(prod: Producer): PimpedProducer = new PimpedProducer(prod)

  implicit def job2PimpedJob(job: Job): PimpedJob = new PimpedJob(job)

  implicit def jobList2PimpedListJob(list: List[Job]): PimpedListJob = new PimpedListJob(list)

  implicit def string2pimpedString(str: String) = new PimpedString(str)

  implicit def stringTuple2ProducedTuple(tuple: (String, String)): (Produced, Produced) = tuple match {
    case (name1, name2) => {
      val produced1 : Produced = Context.produceds.filter(_.name equals name1).headOption.
        getOrElse(throw new IllegalArgumentException(s"there is no produced named $name1"))
      val produced2 : Produced = Context.produceds.filter(_.name equals name2).headOption.
        getOrElse(throw new IllegalArgumentException(s"there is no produced named $name2"))
      (produced1, produced2)
    }
  }

  class PimpedProducer(val prod: Producer) {
    def as(name: String): WithName = (prod, name)
  }

  class PimpedJob(val job: Job) {
    def and(j: Job) = List(job, j)
  }

  class PimpedListJob(val list: List[Job]) {
    def and(j: Job) = list :+ j
  }

  class PimpedString(val str: String) {
    def by(that: String) = (str, that)
  }

}
