import API.Utils._
import API.{WikipediaToItemPrefsMapper, WikipediaToUserVectorReducer}
import org.apache.hadoop.mapred._
import org.apache.mahout.math.{VectorWritable, VarLongWritable}

/**
 * Created by arthur on 06/04/15.
 */
object Main {
  def main(args: Array[String]): Unit = {
    generateUserVectors()

  }

  def generateUserVectors() = {
    val inputPath = "/home/arthur/dslminig/nMiners/src/data/new_file.txtac"
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
    conf setOutputFormat classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]]

    //    conf setJar "hadoop.jar"
    conf setCompressMapOutput true

    FileInputFormat setInputPaths(conf, inputPath)
    FileOutputFormat setOutputPath(conf, outPutPath)

    JobClient runJob conf
  }

  def coocurrence() = {
    val inputPath = "/home/arthur/dslminig/nMiners/src/data/new_file.txtac"
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
    conf setOutputFormat classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]]

    //    conf setJar "hadoop.jar"
    conf setCompressMapOutput true

    FileInputFormat setInputPaths(conf, inputPath)
    FileOutputFormat setOutputPath(conf, outPutPath)

    JobClient runJob conf
  }
}
