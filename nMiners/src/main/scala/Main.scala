import api._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import utils.MapReduceUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat, TextOutputFormat}
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable
import org.apache.mahout.cf.taste.hadoop.item.{VectorAndPrefsWritable, VectorOrPrefWritable}
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, VectorWritable}

/**
 * Created by arthur on 06/04/15.
 */
object Main {

//  def generateMap() = {
//    val inputPath = "src/test/data/input.dat"
//    val outPutPath = "src/outputMap"
//
//    MapReduceUtils.runMap("First Phase",classOf[WikipediaToItemPrefsMapper],
//      classOf[VarLongWritable],classOf[VarLongWritable],
//      classOf[TextInputFormat],classOf[TextOutputFormat[VarLongWritable, VectorWritable]],inputPath,outPutPath,true)
//  }

  def main(args: Array[String]): Unit = {
    generateUserVectors(args(0),args(1))

    //coocurrence()
    //prepare()
    //multiply()
  //  itemsToRecommendFor.
  }

  def generateUserVectors(inputPath:String,outputPath:String) = {
   // val inputPath = "src/test/data/data_2/input_test_level1.txt"
    //val outPutPath = "src/output"
//    MapReduceUtils.runJob("First Phase",classOf[WikipediaToItemPrefsMapper],classOf[WikipediaToUserVectorReducer],
//      classOf[VarLongWritable],classOf[VarLongWritable],classOf[VarLongWritable],classOf[VectorWritable],
//      classOf[TextInputFormat],classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]],inputPath,outputPath,true)
      val HADOOP_HOME = System.getenv("HADOOP_HOME");
      println("====================================================" + HADOOP_HOME)
     val conf = new JobConf(new Configuration())
      conf setQuietMode true

     val job: Job = new Job(conf, "First Phase")
      job.setJarByClass(this.getClass)

      job.setMapperClass(classOf[WikipediaToItemPrefsMapper])
      job.setReducerClass(classOf[WikipediaToUserVectorReducer])

      //Set Map Output values.
      job.setMapOutputKeyClass(classOf[VarLongWritable])
      job.setMapOutputValueClass(classOf[VarLongWritable])

      job.setOutputKeyClass(classOf[VarLongWritable])
      job.setOutputValueClass(classOf[VectorWritable])

      //Set the input and output.
      job.setInputFormatClass(classOf[TextInputFormat])
      job.setOutputFormatClass(classOf[TextOutputFormat[VarLongWritable, VectorWritable]])

      //Set the input and output path


      conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
      conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
      conf.addResource(new Path("/usr/local/hadoop/conf/mapred-site.xml"));
      conf.set("fs.defaultFS", "hdfs://hadoop-node-1:9000/");

    conf.reloadConfiguration()
    val fileSystem = FileSystem.get(conf);
    val listStatus = fileSystem.globStatus(new Path(inputPath));
//    fileSystem.
    for (i <- 0 until listStatus.size) {
      println(listStatus(i).getPath())
    }

    println(fileSystem.getHomeDirectory().getName())
    println(fileSystem.exists(new Path(inputPath)))
    println(fileSystem.getWorkingDirectory().getName())

    FileInputFormat.addInputPath(job, listStatus(0).getPath)
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    job.waitForCompletion(true)

  }

  def coocurrence() = {
    val inputPath = "src/output/part-r-00000"
    val outPutPath = "src/output1"

    MapReduceUtils.runJob("First Phase",classOf[UserVectorToCooccurrenceMapper],classOf[UserVectorToCooccurenceReduce],
      classOf[VarIntWritable],classOf[VarIntWritable],classOf[VarIntWritable],classOf[VectorWritable],
      classOf[SequenceFileInputFormat[VarLongWritable, VectorWritable]],classOf[SequenceFileOutputFormat[VarIntWritable,VectorWritable]],inputPath,outPutPath,true)
  }


  def prepare() = {

    val inputPath1 = "src/output1/part-r-00000"
    val inputPath2 = "src/output/part-r-00000"

    val outPutPath = "src/output2"

    MapReduceUtils.run2MappersJob("Prepare",classOf[CooccurrenceColumnWrapperMapper],classOf[UserVectorSplitterMapper],   classOf[ToVectorAndPrefReducer],
      mapOutputKeyClass = classOf[VarIntWritable],mapOutputValueClass = classOf[VectorOrPrefWritable],
      classOf[VarIntWritable], classOf[VectorAndPrefsWritable],
      classOf[SequenceFileInputFormat[VarIntWritable,VectorWritable]],classOf[SequenceFileInputFormat[VarLongWritable,VectorWritable]],
      classOf[SequenceFileOutputFormat[VarIntWritable,VectorAndPrefsWritable]],inputPath1,inputPath2,outPutPath,true)

  }

  def multiply() = {

    val inputPath = "src/output2/part-r-00000"
    val outPutPath = "src/output3"

    val job = MapReduceUtils.prepareJob("Prepare",classOf[PartialMultiplyMapper],classOf[AggregateAndRecommendReducer],
      classOf[VarLongWritable], classOf[VectorWritable],
      classOf[VarLongWritable], classOf[RecommendedItemsWritable],
      classOf[SequenceFileInputFormat[VarIntWritable,VectorAndPrefsWritable]],
      classOf[TextOutputFormat[VarLongWritable,RecommendedItemsWritable]],inputPath,outPutPath)

    var conf : Configuration = job getConfiguration ()
    conf.set(AggregateAndRecommendReducer.ITEMID_INDEX_PATH,"")
    conf.setInt(AggregateAndRecommendReducer.NUM_RECOMMENDATIONS, 10)

    MapReduceUtils.deleteFolder(outPutPath,conf)
    job.waitForCompletion(true)
  }


}
