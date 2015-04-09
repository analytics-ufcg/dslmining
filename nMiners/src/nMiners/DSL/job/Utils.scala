package DSL.job

object Utils {
  def in_parallel(jobs: List[Job]) = new Parallel(jobs)

  def produce(tuple: (Producer, String)): Job = produce(tuple._1, tuple._2)

  def produce(producer: Producer, name: String): Job = {
    Context.produceds += new Produced(name)
    producer
  }

  def produce(producer: Producer): Job = produce(producer, producer.getClass.getSimpleName)

  def multiply(tuple: (Produced, Produced)): Job = multiply(tuple._1, tuple._2)

  def multiply(a: Produced, b: Produced): Job = new Multiplier(a, b)

  def run = Context.jobs.foreach(_.run)
}