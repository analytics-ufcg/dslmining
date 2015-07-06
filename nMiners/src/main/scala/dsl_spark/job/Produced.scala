package dsl_spark.job

class Produced[A] (val name: String, val producer: Producer[_]) {
  var product : A = _
}
