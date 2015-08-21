package dsl.job

case class Produced[A] (val name: String) {
  var product : A = _
  var producer: Producer[A] = _

  def this(name: String,  producer:Producer[A]) = {
    this(name)
    this.producer = producer
  }
}
