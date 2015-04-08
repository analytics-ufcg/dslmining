package DSL.job

trait Job {
  var pathToOutput = "default"

  def then(job: Job): Job = {
    Context.jobs += this
    job
  }

  def write_on(path: String) = {
    pathToOutput = path
    this
  }

  def run()

  def then (exec: execute.type) = {
    Context.jobs += this
    Context.jobs.foreach(_.run)
  }
}

object execute

class Producer extends Job {
  override def run() = println("rodando Producer")
}

class Applier extends Job {
  override def run() = println("rodando Applier")
}

class Consumer extends Job {
  override def run() = println("rodando Consumer")
}

class ParallelJobs(val jobs: List[Job]) extends Job {

  override def run() = {
    println("rodando PARALEL")
    jobs.foreach(_.run)
    println("terminei PARALEL")
  }
}

object parse_data extends Applier {
  def on(path: String): Job = this
  override def run() = println("rodando parse_data")
}

object preference_item_vector extends Producer {
  override def run() = println("rodando preference_item_vector")
}

object coocurrence_matrix extends Producer {
  override def run() = println("rodando coocurrence_matrix")
}

object user_vector extends Producer {
  override def run() = println("rodando user_vector")
}

object recommendation extends Producer {
  override def run() = println("rodando recommendation")
}