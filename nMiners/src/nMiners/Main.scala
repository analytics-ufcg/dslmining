import java.util.Iterator

import Utils.Implicits
import Implicits._
import API._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, MultipleInputs}
import org.apache.mahout.cf.taste.hadoop.item.{UserVectorSplitterMapper, SimilarityMatrixRowWrapperMapper, VectorOrPrefWritable}
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob
import org.apache.mahout.math.{VarIntWritable, VectorWritable, VarLongWritable}

/**
 * Created by arthur on 06/04/15.
 */
object Main {
  def main(args: Array[String]): Unit = {
    //generateUserVectors()
    //coocurrence()
    //prepare()
  }


//  def generateUserVectors() = {
//    val inputPath = "src/test/data/input_test_level1.txt"
//    val outPutPath = "src/output"
//
//    val conf = new JobConf(classOf[WikipediaToItemPrefsMapper])
//    conf setJobName "wiki parser"
//
//    conf setOutputKeyClass classOf[VarLongWritable]
//    conf setOutputValueClass classOf[VectorWritable]
//
//    conf setMapOutputKeyClass classOf[VarLongWritable]
//    conf setMapOutputValueClass  classOf[VarLongWritable]
//
//    conf setMapperClass classOf[WikipediaToItemPrefsMapper]
//    conf setReducerClass classOf[WikipediaToUserVectorReducer]
//
//    conf setInputFormat classOf[TextInputFormat]
////    conf setOutputFormat classOf[TextOutputFormat[VarLongWritable, VectorWritable]]
//    conf setOutputFormat classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]]
//
//    //    conf setJar "hadoop.jar"
//    conf setCompressMapOutput true
//
//    FileInputFormat setInputPaths(conf, inputPath)
//    FileOutputFormat setOutputPath(conf, outPutPath)
//
//    JobClient runJob conf
//  }
//
//  def coocurrence() = {
//    val inputPath = "src/output/part-00000"
//    val outPutPath = "src/output1"
//
//    val conf = new JobConf(classOf[WikipediaToItemPrefsMapper])
//    conf setJobName "wiki parser"
//
//    conf setOutputKeyClass classOf[VarIntWritable]
//    conf setOutputValueClass classOf[VectorWritable]
//
//    conf setMapOutputKeyClass classOf[VarIntWritable]
//    conf setMapOutputValueClass  classOf[VarIntWritable]
//
//    conf setMapperClass classOf[UserVectorToCooccurrenceMapper]
//    conf setReducerClass classOf[UserVectorToCooccurenceReduce]
//
//    conf setInputFormat classOf[SequenceFileInputFormat[VarLongWritable, VectorWritable]]
////    conf setOutputFormat classOf[TextOutputFormat[VarLongWritable, VectorWritable]]
//    conf setOutputFormat classOf[SequenceFileOutputFormat[VarIntWritable, VectorWritable]]
//
//    //    conf setJar "hadoop.jar"
//    conf setCompressMapOutput true
//
//    FileInputFormat setInputPaths(conf, inputPath)
//    FileOutputFormat setOutputPath(conf, outPutPath)
//
//    JobClient runJob conf
//  }
//
//
//  def prepare() = {
//
//    val inputPath = "src/test/data/input_test_level3.txt"
//    val outPutPath = "src/output2"
//
//    val conf = new JobConf();
//
//
//    val job = new Job(conf)
//    MultipleInputs.addInputPath(job, inputPath,
//      classOf[SequenceFileInputFormat[VarIntWritable,VectorWritable]], classOf[CooccurrenceColumnWrapperMapper])
//    MultipleInputs.addInputPath(job, new Path(prepPath, PreparePreferenceMatrixJob.USER_VECTORS),
//      classOf[SequenceFileInputFormat[_, _]], classOf[UserVectorSplitterMapper])
//
//
//    conf setJobName "wiki parser"
//
//    conf setOutputKeyClass classOf[VarIntWritable]
//    conf setOutputValueClass classOf[Iterator[VectorOrPrefWritable]]
//
//    conf setMapOutputKeyClass classOf[VarIntWritable]
//    conf setMapOutputValueClass  classOf[VectorOrPrefWritable]
//
//    conf setMapperClass classOf[CooccurrenceColumnWrapperMapper]
//
//    conf setInputFormat classOf[SequenceFileInputFormat[VarIntWritable, VectorWritable]]
//    conf setOutputFormat classOf[TextOutputFormat[VarIntWritable,Iterator[VectorOrPrefWritable]]]
//    //conf setOutputFormat classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]]
//
//    //    conf setJar "hadoop.jar"
//    conf setCompressMapOutput true
//
//    FileInputFormat setInputPaths(conf, inputPath)
//    FileOutputFormat setOutputPath(conf, outPutPath)
//
//    JobClient runJob conf
  //}
}
