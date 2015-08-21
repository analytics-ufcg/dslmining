package utils

import dsl.notification.NotificationEndServer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, MultipleInputs}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import utils.Implicits._

object MapReduceUtils {

  /**
   * This methods only sets and runs the Job's Map Class
   * @param jobName
   * @param mapperClass
   * @param outputKeyClass
   * @param outputValueClass
   * @param inputFormatClass
   * @param outputFormatClass
   * @param inputPath
   * @param outputPath
   * @param deleteFolder
   * @param numMapTasks
   * @return
   */
  def runMap(jobName: String,
             mapperClass: Class[_ <: Mapper[_, _, _, _]],
             outputKeyClass: Class[_],
             outputValueClass: Class[_],
             inputFormatClass: Class[_ <: FileInputFormat[_, _]],
             outputFormatClass: Class[_ <: FileOutputFormat[_, _]],
             inputPath: String,
             outputPath: String,
             deleteFolder: Boolean,
             numMapTasks: Option[Int] = None)
            (implicit aSync: Boolean = false) = {


    val conf = new JobConf(new Configuration())

    conf setQuietMode true

    conf set("HADOOP_ROOT_LOGGER", "WARN,console")

    numMapTasks match {
      case Some(num) => conf setNumReduceTasks num
      case _ => {}
    }

    val job: Job = new Job(conf, jobName)

    //Set Mapper and Reducer Classes
    job.setMapperClass(mapperClass)

    job.setOutputKeyClass(outputKeyClass)
    job.setOutputValueClass(outputValueClass)

    //Set the input and output.
    job.setInputFormatClass(inputFormatClass)
    job.setOutputFormatClass(outputFormatClass)

    //Set the input and output path
    FileInputFormat.addInputPath(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)

    if (deleteFolder) this.deleteFolder(outputPath, conf)

    if (aSync) {
      NotificationEndServer configureServer conf
      job.submit
      NotificationEndServer addJobToNotification job.getJobID.toString
    } else {
      job waitForCompletion true
    }
  }


  def deleteFolder(outputPath: String, conf: Configuration) = {
    //Delete the output path before run, to avoid exception
    val fs1: FileSystem = FileSystem.get(conf)
    fs1.delete(outputPath, true)
  }

  /**
   * This methods sets and runs two Mappers
   * @param jobName
   * @param mapper1Class
   * @param mapper2Class
   * @param reducerClass
   * @param mapOutputKeyClass
   * @param mapOutputValueClass
   * @param outputKeyClass
   * @param outputValueClass
   * @param inputFormat1Class
   * @param inputFormat2Class
   * @param outputFormatClass
   * @param inputPath1
   * @param inputPath2
   * @param outputPath
   * @param deleteFolder
   * @param numMapTasks
   * @return
   */
  def run2MappersJob(jobName: String,
                     mapper1Class: Class[_ <: Mapper[_, _, _, _]],
                     mapper2Class: Class[_ <: Mapper[_, _, _, _]],
                     reducerClass: Class[_ <: Reducer[_, _, _, _]],
                     mapOutputKeyClass: Class[_],
                     mapOutputValueClass: Class[_],
                     outputKeyClass: Class[_],
                     outputValueClass: Class[_],
                     inputFormat1Class: Class[_ <: FileInputFormat[_, _]],
                     inputFormat2Class: Class[_ <: FileInputFormat[_, _]],
                     outputFormatClass: Class[_ <: FileOutputFormat[_, _]],
                     inputPath1: String,
                     inputPath2: String,
                     outputPath: String,
                     deleteFolder: Boolean,
                     numMapTasks: Option[Int] = None)
                    (implicit aSync: Boolean = false) = {

    val conf = new JobConf(new Configuration())
    conf setQuietMode true

    conf set("HADOOP_ROOT_LOGGER", "WARN,console")

    numMapTasks match {
      case Some(num) => conf setNumReduceTasks num
      case _ => {}
    }

    val job: Job = new Job(conf, jobName)

    job.setReducerClass(reducerClass)

    job.setOutputKeyClass(outputKeyClass)
    job.setOutputValueClass(outputValueClass)

    job.setMapOutputKeyClass(mapOutputKeyClass)
    job.setMapOutputValueClass(mapOutputValueClass)

    //Set the input and output.
    job.setOutputFormatClass(outputFormatClass)

    job.setJarByClass(this.getClass)

    //Set the input and output path
    MultipleInputs.addInputPath(job, inputPath1, inputFormat1Class, mapper1Class)
    MultipleInputs.addInputPath(job, inputPath2, inputFormat2Class, mapper2Class)

    FileOutputFormat.setOutputPath(job, outputPath)

    if (deleteFolder) this.deleteFolder(outputPath, conf)

    if (aSync) {
      NotificationEndServer configureServer conf
      job.submit
      NotificationEndServer addJobToNotification job.getJobID.toString
    } else {
      job waitForCompletion true
    }
  }

  /**
   * This method prepares and runs the Job
   * @param jobName
   * @param mapperClass
   * @param reducerClass
   * @param mapOutputKeyClass
   * @param mapOutputValueClass
   * @param outputKeyClass
   * @param outputValueClass
   * @param inputFormatClass
   * @param outputFormatClass
   * @param inputPath
   * @param outputPath
   * @param deleteFolder
   * @param numMapTasks
   * @return
   */
  def runJob(jobName: String,
             mapperClass: Class[_ <: Mapper[_, _, _, _]],
             reducerClass: Class[_ <: Reducer[_, _, _, _]],
             mapOutputKeyClass: Class[_],
             mapOutputValueClass: Class[_],
             outputKeyClass: Class[_],
             outputValueClass: Class[_],
             inputFormatClass: Class[_ <: FileInputFormat[_, _]],
             outputFormatClass: Class[_ <: FileOutputFormat[_, _]],
             inputPath: String,
             outputPath: String,
             deleteFolder: Boolean,
             numMapTasks: Option[Int] = None)
            (implicit aSync: Boolean = false) = {
    val job: Job = prepareJob(jobName, mapperClass, reducerClass, mapOutputKeyClass, mapOutputValueClass,
      outputKeyClass, outputValueClass, inputFormatClass, outputFormatClass, inputPath, outputPath, numMapTasks)
    val conf = job getConfiguration()
    if (deleteFolder) this.deleteFolder(outputPath, conf)

    if (aSync) {
      NotificationEndServer configureServer conf
      job.submit
      NotificationEndServer addJobToNotification job.getJobID.toString
    } else {
      job waitForCompletion true
    }
  }


  /**
   * This method sets all the job's needed configuration
   * @param jobName
   * @param mapperClass
   * @param reducerClass
   * @param mapOutputKeyClass
   * @param mapOutputValueClass
   * @param outputKeyClass
   * @param outputValueClass
   * @param inputFormatClass
   * @param outputFormatClass
   * @param inputPath
   * @param outputPath
   * @param numReduceTasks
   * @return
   * Configured Job
   */
  def prepareJob(jobName: String,
                 mapperClass: Class[_ <: Mapper[_, _, _, _]],
                 reducerClass: Class[_ <: Reducer[_, _, _, _]],
                 mapOutputKeyClass: Class[_], mapOutputValueClass: Class[_],
                 outputKeyClass: Class[_], outputValueClass: Class[_],
                 inputFormatClass: Class[_ <: FileInputFormat[_, _]],
                 outputFormatClass: Class[_ <: FileOutputFormat[_, _]],
                 inputPath: String, outputPath: String,
                 numReduceTasks: Option[Int] = None): Job = {
    val conf = new JobConf(new Configuration())
    conf setQuietMode true
    conf set("HADOOP_ROOT_LOGGER", "WARN,console")

    numReduceTasks match {
      case Some(num) => conf setNumReduceTasks num
      case _ => {}
    }

    val job: Job = new Job(conf, jobName)

    //Set Mapper and Reducer Classes
    job.setMapperClass(mapperClass)
    job.setReducerClass(reducerClass)

    //Set Map Output values.
    job.setMapOutputKeyClass(mapOutputKeyClass)
    job.setMapOutputValueClass(mapOutputValueClass)

    job.setOutputKeyClass(outputKeyClass)
    job.setOutputValueClass(outputValueClass)

    //Set the input and output.
    job.setInputFormatClass(inputFormatClass)
    job.setOutputFormatClass(outputFormatClass)

    //Set the input and output path
    FileInputFormat.addInputPath(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    job.setJarByClass(this.getClass)
    job
  }
}
