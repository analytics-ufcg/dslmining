package Utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.{Reducer, Mapper, Job}
import Implicits._
import org.apache.hadoop.mapreduce.lib.input.{MultipleInputs, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 * Created by andryw on 08/04/15.
 */
object MapReduceUtils {

  def deleteFolder(outputPath:String, conf:Configuration) = {
    //Delete the output path before run, to avoid exception
    val fs1:FileSystem = FileSystem.get(conf);
    fs1.delete(outputPath, true);
  }

  def run2MappersJob(jobName:String,mapper1Class:Class[_<:Mapper[_,_,_,_]],  mapper2Class:Class[_<:Mapper[_,_,_,_]],
                     outputKeyClass:Class[_],  outputValueClass:Class[_],
             inputFormat1Class:Class[_<:FileInputFormat[_,_]], inputFormat2Class:Class[_<:FileInputFormat[_,_]],
        outputFormatClass:Class[_<:FileOutputFormat[_,_]],   inputPath1:String, inputPath2:String, outputPath:String,deleteFolder:Boolean) = {


    var conf : Configuration = new Configuration();

    var job: Job = new Job(conf,jobName);

    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);

    //Set the input and output.
    job.setOutputFormatClass(outputFormatClass);

    //Set the input and output path
    MultipleInputs.addInputPath(job,inputPath1,inputFormat1Class,mapper1Class);
    MultipleInputs.addInputPath(job,inputPath2,inputFormat2Class,mapper2Class);

    FileOutputFormat.setOutputPath(job, outputPath);

    if (deleteFolder) this.deleteFolder(outputPath,conf);

    job.waitForCompletion(true);
  }


  def runJob(jobName:String,mapperClass:Class[_<:Mapper[_,_,_,_]],  reducerClass:Class[_<:Reducer[_,_,_,_]],  mapOutputKeyClass:Class[_],
     mapOutputValueClass:Class[_],  outputKeyClass:Class[_],  outputValueClass:Class[_],
     inputFormatClass:Class[_<:FileInputFormat[_,_]], outputFormatClass:Class[_<:FileOutputFormat[_,_]],   inputPath:String, outputPath:String,deleteFolder:Boolean) = {
    var conf : Configuration = new Configuration();

    var job: Job = new Job(conf,jobName);

    //Set Mapper and Reducer Classes
    job.setMapperClass(mapperClass);
    job.setReducerClass(reducerClass);

    //Set Map Output values.
    job.setMapOutputKeyClass(mapOutputKeyClass);
    job.setMapOutputValueClass(mapOutputValueClass);

    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);

    //Set the input and output.
    job.setInputFormatClass(inputFormatClass);
    job.setOutputFormatClass(outputFormatClass);



    //Set the input and output path
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    if (deleteFolder) this.deleteFolder(outputPath,conf);

    job.waitForCompletion(true);
  }
}
