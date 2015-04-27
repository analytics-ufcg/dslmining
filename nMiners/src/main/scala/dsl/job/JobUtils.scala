package dsl.job

object JobUtils {
  def in_parallel(jobs: List[Job]) = {
    new Parallel(jobs)
  }

  def produce(tuple: (Producer, String)): Job = produce(tuple._1, tuple._2)

  def produce(producer: Producer, name: String): Job = {
    val produced = new Produced(name)
    Context.produceds += produced
    producer.produced = produced
    producer
  }

  def produce(producer: Producer): Job = produce(producer, producer.name.replaceAll("\\$",""))

  def multiply(tuple: (Produced, Produced)): Job = multiply(tuple._1, tuple._2)

  def multiply(a: Produced, b: Produced): Job = new Multiplier(a, b)

  def run = Context.jobs.foreach(_.run)
}