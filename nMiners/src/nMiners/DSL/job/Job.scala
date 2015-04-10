package DSL.job

import API.{UserVectorToCooccurenceReduce, UserVectorToCooccurrenceMapper, WikipediaToItemPrefsMapper, WikipediaToUserVectorReducer}
import Utils.MapReduceUtils.runJob
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, SequenceFileOutputFormat}
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, VectorWritable}

trait Job {
  var name: String

  var pathToOutput = "data/test"

  var pathToInput = ""

  def then(job: Job): Job = {
    job.pathToInput = pathToOutput + "/part-r-00000"
    Context.jobs += this
    job
  }

  def write_on(path: String) = {
    pathToOutput = path
    this
  }

  def run() = Console.err.println(s"\n\nRunning: $name")

  def then(exec: execute.type) = {
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
    Console.err.println("\n\nRunning in parallel\n{")
    jobs.foreach(_.run)
    Console.err.println("}")
  }

  override def then(job : Job) = {
    val ret = super.then(job)
    jobs foreach {_.pathToInput = pathToOutput + "/part-r-00000"}
    ret
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

  override def run = {
    super.run
    runJob(name, mapperClass = classOf[WikipediaToItemPrefsMapper], reducerClass = classOf[WikipediaToUserVectorReducer],
      mapOutputKeyClass = classOf[VarLongWritable], mapOutputValueClass = classOf[VarLongWritable],
      outputKeyClass = classOf[VarLongWritable], outputValueClass = classOf[VectorWritable],
      inputFormatClass = classOf[TextInputFormat],
      outputFormatClass = classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]],
      inputPath = path, outputPath = pathToOutput, deleteFolder = false)
  }
}

object coocurrence_matrix extends Producer {
  override var name: String = this.getClass.getSimpleName

  pathToOutput = "data/test2"

  override def run = {
    super.run

    runJob(name, mapperClass = classOf[UserVectorToCooccurrenceMapper],
      reducerClass = classOf[UserVectorToCooccurenceReduce], mapOutputKeyClass = classOf[VarIntWritable],
      mapOutputValueClass = classOf[VarIntWritable], outputKeyClass = classOf[VarIntWritable],
      outputValueClass = classOf[VectorWritable],
      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VarIntWritable]],
      outputFormatClass = classOf[TextOutputFormat[VarIntWritable, VectorWritable]], pathToInput, pathToOutput,
      deleteFolder = false)
  }
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