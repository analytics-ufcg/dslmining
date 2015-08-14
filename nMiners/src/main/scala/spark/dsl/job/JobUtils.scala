package spark.dsl.job

import org.apache.mahout.math.drm.DrmLike

object JobUtils {
  def in_parallel(jobs: List[Job]) = {
    new Parallel(jobs)
  }

  def produce(tuple: (Producer[_], String)): Job = produce(tuple._1, tuple._2)

  def produce(producer: Producer[_], name: String): Job = {
    producer.produced = new Produced(name)
    Context.produceds += producer.produced
//    producer.produced = produced
    producer
  }

  def produce(producer: Producer[_]): Job = produce(producer, producer.name.replaceAll("\\$",""))

  def multiply(tuple: (Produced[DrmLike[Int]], Produced[DrmLike[Int]])): Job = multiply(tuple._1, tuple._2)

  def multiply(a: Produced[DrmLike[Int]], b: Produced[DrmLike[Int]]): Job = new Multiplier(a, b)

  def run = Context.jobs.foreach(_.run)
}