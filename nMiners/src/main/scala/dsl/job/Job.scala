package dsl.job

import java.io.File
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{Files, Paths}
import java.util.concurrent.CountDownLatch

import api._
import dsl.notification.NotificationEndServer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, VectorWritable}
import utils.MapReduceUtils

/**
 * Job is a trait that produce results
 */
trait Job {

  val defaultOutPath = ""

  var name: String

  var numProcess: Option[Int] = None

  var pathToOutput: Option[String] = None

  var pathToInput = ""

  var pathToOut = defaultOutPath

  implicit var aSync = false

  def then(job: Job): Job = {
    job.pathToInput = pathToOutput.getOrElse("") + "/part-r-00000"
    Context.jobs += this
    job
  }

  def afterJob() = {}

  private def after() = {

    if (this.getClass().isInstance(recommendation)) {
      val outputFile = new File(pathToOutput.get)
      outputFile getParentFile() mkdirs()
      Files.copy(Paths.get(pathToInput + "/part-r-00000"), Paths.get(pathToOutput.get), REPLACE_EXISTING)

    }

    this.afterJob()
  }

  // Write on path
  def write_on(path: String) = {
    pathToOutput = Some(path)
    Context.addFinalOutput(path)
    this
  }

  // Execute the Job
  def run() = {
    //Context.paths(this.name) = pathToOutput.getOrElse("")  + "/part-r-00000"
    this match {
      case prod: Producer => {
        prod.generateOutputPath()
        val some = Option(prod.produced)
        some match {
          case Some(produced) => Context.paths(produced.name) = pathToOutput.getOrElse("") + "/part-r-00000"
          case None =>
        }

      }

      case _ =>

    }
    Console.err.println(s"\n\nRunning: $name")
  }

  private def exec() = {
    run()
    after()
  }

  // Run all jobs
  def then(exec: execute.type) = {
    Context.jobs += this
    var lastOutput = ""
    Context.jobs.foreach(job => {
      if (lastOutput != "")
        job.pathToInput = lastOutput
      job.exec()
      lastOutput = job.pathToOutput.getOrElse("")
    })
    NotificationEndServer.stop
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
abstract class Producer extends Job {
  var produced: Produced

  def generateOutputPath(): Unit = {
    this.pathToOutput match {
      case None => this.pathToOutput = Some(Context.basePath + "/data_" + produced.name)
      case _ =>
    }
  }

}

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

  def notificationIn(latch: CountDownLatch) = (jobId: String, status: String) => {
    println("\n\n\n\n\n\n\n\n\n\n\n\n")
    println(status)
    println("\n\n\n\n\n\n\n\n\n\n\n\n")
    latch.countDown()
  }

  // Run each job
  override def run() = {
    NotificationEndServer.start
    val jobsRunnedCountDown = new CountDownLatch(jobs.size)
    NotificationEndServer addNotificationFunction notificationIn(jobsRunnedCountDown)
    jobs.foreach(job => {
      job.aSync = true
      job.run()
    })

    jobsRunnedCountDown.await
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

  def clear() = {
    Context.clearQueues()
  }

  // Get a data file
  def on(path: String): Job = {
    clear()
    this.path = path
    name = this.getClass.getSimpleName + s" on $path"
    Context.addInputPath(path)
    this
  }

  override var name: String = ""

  // Run the job
  override def run() = {
    super.run()
  }
}

/**
 * Is a object that can produce similarity matrix that can be used anytime
 */
object similarity_matrix extends Producer {
  override var produced: Produced = _
  override var name: String = this.getClass.getSimpleName
  var similarity: SimilarityType = null;

  // pathToOutput = Context.basePath + "/data/test2"

  def using(similarity: SimilarityType): Producer = {
    this.similarity = similarity
    this
  }

  override def run() = {
    super.run()

//    MatrixGenerator.runJob(pathToInput, pathToOutput.get, inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VarIntWritable]],
//      outputFormatClass = classOf[SequenceFileOutputFormat[VarIntWritable, VectorWritable]], deleteFolder = true, numReduceTasks = numProcess)

    val BASE_PATH = pathToOutput.get
    pathToOutput = Some (pathToOutput.get + "/matrix")
    Context.paths(produced.name) = pathToOutput.getOrElse("") + "/part-r-00000"



    RowSimilarityJobAnalytics.runJob(pathToInput + "/part-r-00000",
      pathToOutput.get,
      classOf[SequenceFileInputFormat[VarLongWritable,VectorWritable]],
      classOf[SequenceFileOutputFormat[IntWritable,VectorWritable]],true,similarityClassnameArg = this.similarity._type,basePath =  BASE_PATH)
  }

}

/**
 * Is a object that can produce user vectors that can be used anytime
 */
object user_vector extends Producer {
  override var produced: Produced = _
  override var name: String = this.getClass.getSimpleName
  // pathToOutput = Context.basePath + "/data_" + this.produced.name

  // Run the job
  override def run() = {
    super.run()

    pathToInput = Context.getInputPath()

    UserVectorGenerator.runJob(pathToInput, pathToOutput.get, inputFormatClass = classOf[TextInputFormat],
      outputFormatClass = classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]], deleteFolder = true, numReduceTasks = numProcess)
  }
}


object recommendation extends Producer {
  override var name: String = this.getClass.getSimpleName
  override var produced: Produced = _
}

/** Multiplier is class which produce a consumer job.
  * @param producedOne Produce one
  * @param producedTwo Produce two
  */

class Multiplier(val producedOne: Produced, val producedTwo: Produced) extends Consumer {
  override var name: String = this.getClass.getSimpleName + s" $producedOne by $producedTwo"

  //pathToOutput = Context.basePath + "/data/test4"
  //val pathToOutput1 = Context.basePath + "/data/test3"

  //  val path1 = Context.basePath + "/data/test2/part-r-00000"
  //  val path2 = Context.basePath + "/data/test/part-r-00000"
  //
  //


  // Run the job
  override def run() = {
    val path1 = Context.paths(producedOne.name)
    val path2 = Context.paths(producedTwo.name)
    super.run()
    val BASE = pathToOutput match {
      case None => Context.basePath
      case Some(path) => path
    }
    val pathToOutput1 = BASE + "/data_prepare"
    PrepareMatrixGenerator.runJob(inputPath1 = path1, inputPath2 = path2, outPutPath = pathToOutput1,
      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VectorWritable]],
      outputFormatClass = classOf[SequenceFileOutputFormat[VarIntWritable, VectorAndPrefsWritable]],
      deleteFolder = true, numMapTasks = numProcess)

    pathToInput = pathToOutput1 + "/part-r-00000"
    val pathToOutput2 = BASE + "/data_multiplied"

    val job = MapReduceUtils.prepareJob(jobName = "Prepare", mapperClass = classOf[PartialMultiplyMapper],
      reducerClass = classOf[AggregateAndRecommendReducer], mapOutputKeyClass = classOf[VarLongWritable],
      mapOutputValueClass = classOf[VectorWritable],
      outputKeyClass = classOf[VarLongWritable], outputValueClass = classOf[RecommendedItemsWritable],
      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VectorAndPrefsWritable]],
      outputFormatClass = classOf[TextOutputFormat[VarLongWritable, RecommendedItemsWritable]],
      pathToInput, pathToOutput2, numMapTasks = numProcess)

    var conf: Configuration = job getConfiguration()
    conf.set(AggregateAndRecommendReducer.ITEMID_INDEX_PATH, "")
    conf.setInt(AggregateAndRecommendReducer.NUM_RECOMMENDATIONS, 10)

    MapReduceUtils.deleteFolder(pathToOutput2, conf)
    pathToOutput = Some(pathToOutput2)
    job.waitForCompletion(true)
  }
}

//case class WordCount(val input: String, val output: String) extends Job {
//  override var name: String = "WordCount"
//
//  override def run = {
//    super.run
//    MapReduceUtils.prepareJob(jobName = "Prepare", mapperClass = classOf[WordMap],
//      reducerClass = classOf[AggregateAndRecommendReducer], mapOutputKeyClass = classOf[VarLongWritable],
//      mapOutputValueClass = classOf[VectorWritable],
//      outputKeyClass = classOf[VarLongWritable], outputValueClass = classOf[RecommendedItemsWritable],
//      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VectorAndPrefsWritable]],
//      outputFormatClass = classOf[TextOutputFormat[VarLongWritable, RecommendedItemsWritable]],
//      input, output, numMapTasks = numProcess)
//
//  }
//
//  class WordMap extends Mapper[LongWritable, Text, Text, IntWritable] {
//    override def map(key: LongWritable, text: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context) =
//      text.toString split (" ") foreach ((word: String) => context.write(new Text(word), new IntWritable(1)))
//  }
//
//  class WordReduce extends Reducer[Text, IntWritable, Text, IntWritable] {
//    override def reduce(word: Text, iterator: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context) =
//      context.write(word, iterator.iterator().sum)
//  }
//}