import API.Utils._
import API.{WikipediaToItemPrefsMapper, WikipediaToUserVectorReducer}
import org.apache.hadoop.mapred._
import org.apache.mahout.math.{VectorWritable, VarLongWritable}

/**
 * Created by arthur on 06/04/15.
 */
object Main {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outPutPath = args(1)

    val conf = new JobConf(classOf[WikipediaToItemPrefsMapper])
    conf setJobName "wiki parser"

    conf setOutputKeyClass classOf[VarLongWritable]
    conf setOutputValueClass classOf[VarLongWritable]

    conf setMapperClass classOf[WikipediaToItemPrefsMapper]
    conf setReducerClass classOf[WikipediaToUserVectorReducer]

    conf setInputFormat classOf[TextInputFormat]
    conf setOutputFormat classOf[TextOutputFormat[VarLongWritable, VectorWritable]]

    //    conf setJar "hadoop.jar"
    conf setCompressMapOutput true

    FileInputFormat setInputPaths(conf, inputPath)
    FileOutputFormat setOutputPath(conf, outPutPath)

    JobClient runJob conf
  }
}
