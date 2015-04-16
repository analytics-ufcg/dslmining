package dsl.job

import java.nio.file.{Files, Paths}
import java.nio.file.StandardCopyOption.REPLACE_EXISTING

import api._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, VectorWritable}
import utils.MapReduceUtils
import utils.MapReduceUtils.runJob

/**
 * Job is a trait that produce results
 */
trait Job {

  val defaultOutPath = ""

  var name: String

  var numProcess: Option[Int] = None

  var pathToOutput = "data/test"

  var pathToInput = ""

  var pathToOut = defaultOutPath

  def then(job: Job): Job = {
    job.pathToInput = pathToOutput + "/part-r-00000"
    Context.jobs += this
    job
  }

  def afterJob() = {}

  private def after() = {
    if (!(pathToOut equals defaultOutPath)) {
      Files.copy(Paths.get(pathToOutput + "/part-r-00000"), Paths.get(pathToOut), REPLACE_EXISTING)
    }
    this.afterJob()
  }

  // Write on path
  def write_on(path: String) = {
    pathToOutput = path
    this
  }

  // Execute the Job
  def run() = {
    Console.err.println(s"\n\nRunning: $name")
  }

  private def exec() = {
    run()
    after()
  }

  // Run all jobs
  def then(exec: execute.type) = {
    Context.jobs += this
    Context.jobs.foreach(_.exec())
  }

  // Setup the number of nodes
  def in(nodes: Int) = {
    this.numProcess = Some(nodes)
    this
  }
}

object execute

/**
 * Producer is a class that can produce results that can be used anytime
 */
abstract class Producer extends Job

/**
 * Applier is a class that can produce results that should be used immediately
 */
abstract class Applier extends Job

/**
 * Consumer is a class that consumer others produce
 */
abstract class Consumer extends Job

/**
 * ParallelJobs is a class responsible to run other Jobs
 */
class Parallel(val jobs: List[Job]) extends Job {

  // Run each job
  override def run() = {
    Console.err.println("\n\nRunning in parallel\n{")
    jobs.foreach(_.run())
    Console.err.println("}")
  }

  // Setup the number of nodes
  override def in(nodes: Int) = {
    jobs.foreach(_.in(nodes))
    this
  }

  // Add each job to a queue
  override def then(job: Job) = {
    val newJob = super.then(job)
    job.pathToInput = "data/test3"
    jobs foreach {
      _.pathToInput = pathToOutput + "/part-r-00000"
    }
    newJob
  }

  override var name: String = "Parallel"
}

/**
 * Is a object that can produce a data parse that should be used immediately
 */
object parse_data extends Applier {

  var path = ""

  // Get a data file
  def on(path: String): Job = {
    this.path = path
    name = this.getClass.getSimpleName + s" on $path"
    this
  }

  override var name: String = ""

  // Run the job
  override def run() = {
    super.run()
    runJob(name, mapperClass = classOf[WikipediaToItemPrefsMapper],
      reducerClass = classOf[WikipediaToUserVectorReducer],
      mapOutputKeyClass = classOf[VarLongWritable], mapOutputValueClass = classOf[VarLongWritable],
      outputKeyClass = classOf[VarLongWritable], outputValueClass = classOf[VectorWritable],
      inputFormatClass = classOf[TextInputFormat],
      outputFormatClass = classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]],
      inputPath = path, outputPath = pathToOutput, deleteFolder = false, numMapTasks = numProcess)
  }
}

/**
 * Is a object that can produce coocurrence matrix that can be used anytime
 */
object coocurrence_matrix extends Producer {
  override var name: String = this.getClass.getSimpleName

  pathToOutput = "data/test2"


  // Run the job
  override def run() = {
    super.run()

    runJob(name, mapperClass = classOf[UserVectorToCooccurrenceMapper],
      reducerClass = classOf[UserVectorToCooccurenceReduce], mapOutputKeyClass = classOf[VarIntWritable],
      mapOutputValueClass = classOf[VarIntWritable], outputKeyClass = classOf[VarIntWritable],
      outputValueClass = classOf[VectorWritable],
      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VarIntWritable]],
      outputFormatClass = classOf[SequenceFileOutputFormat[VarIntWritable, VectorWritable]], pathToInput,
      pathToOutput, deleteFolder = true, numMapTasks = numProcess)
  }
}

/**
 * Is a object that can produce user vectors that can be used anytime
 */
object user_vector extends Producer {
  override var name: String = this.getClass.getSimpleName

  pathToOutput = "data/test3"

  // Run the job
  override def run() = {
    super.run()

    val path1 = "data/test2/part-r-00000"
    val path2 = "data/test/part-r-00000"

    PrepareMatrixGenerator.runJob(inputPath1 = path1, inputPath2 = path2, outPutPath = pathToOutput,
      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VectorWritable]],
      outputFormatClass = classOf[SequenceFileOutputFormat[VarIntWritable, VectorAndPrefsWritable]],
      deleteFolder = true, numMapTasks = numProcess)
  }
}

object recommendation extends Producer {
  override var name: String = this.getClass.getSimpleName
}

/** Multiplier is class which produce a consumer job.
  * @param producedOne Produce one
  * @param producedTwo Produce two
  */

class Multiplier(val producedOne: Produced, val producedTwo: Produced) extends Consumer {
  override var name: String = this.getClass.getSimpleName + s" $producedOne by $producedTwo"

  pathToOutput = "data/test4"

  // Run the job
  override def run() = {
    super.run()

    val job = MapReduceUtils.prepareJob(jobName = "Prepare", mapperClass = classOf[PartialMultiplyMapper],
      reducerClass = classOf[AggregateAndRecommendReducer], mapOutputKeyClass = classOf[VarLongWritable],
      mapOutputValueClass = classOf[VectorWritable],
      outputKeyClass = classOf[VarLongWritable], outputValueClass = classOf[RecommendedItemsWritable],
      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VectorAndPrefsWritable]],
      outputFormatClass = classOf[TextOutputFormat[VarLongWritable, RecommendedItemsWritable]],
      pathToInput, pathToOutput, numMapTasks = numProcess)

    var conf: Configuration = job getConfiguration()
    conf.set(AggregateAndRecommendReducer.ITEMID_INDEX_PATH, "")
    conf.setInt(AggregateAndRecommendReducer.NUM_RECOMMENDATIONS, 10)

    MapReduceUtils.deleteFolder(pathToOutput, conf)
    job.waitForCompletion(true)
  }
}