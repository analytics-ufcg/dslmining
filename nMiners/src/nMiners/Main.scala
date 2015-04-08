import API.Utils._
import API.{UserVectorToCooccurenceReduce, UserVectorToCooccurrenceMapper, WikipediaToItemPrefsMapper, WikipediaToUserVectorReducer}
import org.apache.hadoop.mapred._
import org.apache.mahout.math.{VarIntWritable, VectorWritable, VarLongWritable}

/**
 * Created by arthur on 06/04/15.
 */
object Main {
  def main(args: Array[String]): Unit = {
    generateUserVectors()
//      coocurrence()
  }

  def generateUserVectors() = {
    val inputPath = "/home/arthur/dslminig/nMiners/src/test/data/input_test_level2.txt"
    val outPutPath = "/home/arthur/dslminig/nMiners/src/output"

    val conf = new JobConf(classOf[WikipediaToItemPrefsMapper])
    conf setJobName "wiki parser"

    conf setOutputKeyClass classOf[VarLongWritable]
    conf setOutputValueClass classOf[VectorWritable]

    conf setMapOutputKeyClass classOf[VarLongWritable]
    conf setMapOutputValueClass  classOf[VarLongWritable]

    conf setMapperClass classOf[WikipediaToItemPrefsMapper]
    conf setReducerClass classOf[WikipediaToUserVectorReducer]

    conf setInputFormat classOf[TextInputFormat]
    conf setOutputFormat classOf[TextOutputFormat[VarLongWritable, VectorWritable]]
//    conf setOutputFormat classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]]

    //    conf setJar "hadoop.jar"
    conf setCompressMapOutput true

    FileInputFormat setInputPaths(conf, inputPath)
    FileOutputFormat setOutputPath(conf, outPutPath)

    JobClient runJob conf
  }

  def coocurrence() = {
    val inputPath = "/home/arthur/dslminig/nMiners/src/output/part-00000"
    val outPutPath = "/home/arthur/dslminig/nMiners/src/output1"

    val conf = new JobConf(classOf[WikipediaToItemPrefsMapper])
    conf setJobName "wiki parser"

    conf setOutputKeyClass classOf[VarLongWritable]
    conf setOutputValueClass classOf[VectorWritable]

    conf setMapOutputKeyClass classOf[VarIntWritable]
    conf setMapOutputValueClass  classOf[VarIntWritable]

    conf setMapperClass classOf[UserVectorToCooccurrenceMapper]
    conf setReducerClass classOf[UserVectorToCooccurenceReduce]

    conf setInputFormat classOf[SequenceFileInputFormat[VarLongWritable, VectorWritable]]
    conf setOutputFormat classOf[TextOutputFormat[VarLongWritable, VectorWritable]]

    //    conf setJar "hadoop.jar"
    conf setCompressMapOutput true

    FileInputFormat setInputPaths(conf, inputPath)
    FileOutputFormat setOutputPath(conf, outPutPath)

    JobClient runJob conf
  }
}
