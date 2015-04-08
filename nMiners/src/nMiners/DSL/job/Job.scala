package DSL.job

trait Job {
  var name : String

  var pathToOutput = "default"

  def then(job: Job): Job = {
    Context.jobs += this
    job
  }

  def write_on(path: String) = {
    pathToOutput = path
    this
  }

  def run() = println(s"Running: $name")

  def then (exec: execute.type) = {
    Context.jobs += this
    Context.jobs.foreach(_.run)
  }
}

object execute

abstract class Producer extends Job

abstract class Applier extends Job

abstract class Consumer extends Job

class Parallel(val jobs: List[Job]) extends Job {

  override def run() = {
    println("Running in parallel\n{")
    jobs.foreach(_.run)
    println("}")
  }

  override var name: String = "Parallel"
}

object parse_data extends Applier {

  var path = ""

  def on(path: String): Job = {
    this.path = path
    name = this.getClass.getSimpleName + s" on $path"
    this
  }
  override var name: String = ""
}

object preference_item_vector extends Producer {
  override var name: String = this.getClass.getSimpleName
}

object coocurrence_matrix extends Producer {
  override var name: String = this.getClass.getSimpleName

}

object user_vector extends Producer {
  override var name: String = this.getClass.getSimpleName

}

object recommendation extends Producer {
  override var name: String = this.getClass.getSimpleName
}

class Multiplier(val a: Produced, val b: Produced) extends Consumer {
  override var name: String = this.getClass.getSimpleName + s" $a by $b"
}