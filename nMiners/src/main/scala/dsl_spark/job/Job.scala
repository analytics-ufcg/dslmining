package dsl_spark.job

import java.util.concurrent.CountDownLatch

import dsl.notification.NotificationEndServer
import org.apache.hadoop.fs.Path
import org.apache.mahout.math.drm.DrmLike
import org.apache.mahout.math.drm.RLikeDrmOps._
import org.slf4j.{Logger, LoggerFactory}

/**
 * Job is a trait that produce results
 */
trait Job {

  val logger: Logger

  var name: String

  var numProcess: Option[Int] = None

  var pathToOutput: Option[String] = None

  var pathToInput = ""

  implicit var aSync = false

  def then(job: Job): Job = {
    //Set the input path of the next job to the output of the current job.
    //Example: a then b ==> b.input = a.output
    job.pathToInput = pathToOutput.get + "/part-r-00000"
    Context.jobs += this
    job
  }

  def afterJob() = {}

  private def after() = {
    this.afterJob()
  }

  // Write on path
  def write_on(path: String) = {
    pathToOutput = Some(path)
//    Context.addFinalOutput(path)
    this
  }

  // Execute the Job
  def run() = {
  }

  private def exec() = {
    logger.info(s"Running $name job")
    run()
    after()
  }

  // Run all jobs
  def then(exec: execute.type) = {
    try {
      Context.jobs += this
      var lastOutput = ""
      Context.jobs.foreach(job => {
        if (lastOutput != "")
          job.pathToInput = lastOutput
        job.exec()
        lastOutput = job.pathToOutput.getOrElse("")
      })
    } finally {
      NotificationEndServer.stop
    }
  }

  // Setup the number of nodes
  def in(nodes: Int) = {
    this.numProcess = Some(nodes)
    this
  }
}

object execute


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
    if ("SUCCEEDED" equals status) {
      latch.countDown()
    } else {
      NotificationEndServer.stop
      logger.error(s"Job with id $jobId is finished with status $status. Please check your jobs.")
      System.exit(-1)
    }
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
    logger.info("running jobs in paralel")
    jobs foreach {
      _.pathToInput = pathToOutput + "/part-r-00000"
    }
    newJob
  }

  override var name: String = "Parallel"
  override val logger = LoggerFactory.getLogger(classOf[Parallel])
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
    this.pathToInput = path
    this.pathToOutput = Some(new Path(System.getProperty("java.io.tmpdir"), "wikipediaToCSV").toUri().toString)
    name = this.getClass.getSimpleName + s" on $path"
//    Context.addInputPath(path)
    this
  }

  override val logger = LoggerFactory.getLogger(this.getClass())
  override var name: String = ""

  // Run the job
  override def run() = {
    super.run()
//    WikipediaToCSV.runJob(inputPath = path, dirOutputName = pathToOutput.get, deleteFolder = false, numProcess)
  }
}


/** Multiplier is class which produce a consumer job.
  * @param producedOne Produce one
  * @param producedTwo Produce two
  */
class Multiplier(val producedOne: Produced[DrmLike[Int]], val producedTwo: Produced[DrmLike[Int]])
  extends Consumer with Producer[DrmLike[Int]]{

  override var name: String = this.getClass.getSimpleName + s" $producedOne by $producedTwo"
  override val logger = LoggerFactory.getLogger(classOf[Multiplier])

  produced = new Produced[DrmLike[Int]](this.name, this)

  override def run() = {
    super.run()

    produced.product = producedOne.product %*% producedTwo.product
  }
}

case class WordCount(val input: String, val output: String) extends Job {
  override var name: String = "WordCount"
  override val logger = LoggerFactory.getLogger(classOf[WordCount])

  override def run = {
    super.run
//    MapReduceUtils.runJob(jobName = "Prepare", mapperClass = classOf[WordMap],
//      reducerClass = classOf[WordReduce], mapOutputKeyClass = classOf[Text],
//      mapOutputValueClass = classOf[IntWritable],
//      outputKeyClass = classOf[Text], outputValueClass = classOf[IntWritable],
//      inputFormatClass = classOf[TextInputFormat],
//      outputFormatClass = classOf[TextOutputFormat[Text, IntWritable]],
//      inputPath = input, outputPath = output, deleteFolder = true, numMapTasks = numProcess)
  }
}

