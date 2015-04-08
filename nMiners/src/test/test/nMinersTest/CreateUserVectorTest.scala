package nMinersTest

import API.{WikipediaToUserVectorReducer, WikipediaToItemPrefsMapper}
import API.Utils._
import org.apache.hadoop.mapred._
import org.apache.mahout.math.{VectorWritable, VarLongWritable}
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by leonardo on 08/04/15.
 */
class CreateUserVectorTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/data/"
  "Level one" should "execute first mapreduce" in {
    val inputPath = BASE_PHATH+"input_test_level1.txt"
    val namePath = BASE_PHATH+"output_test_level1"; // Path da pasta e nao do arquivo

    val conf = new JobConf(classOf[WikipediaToItemPrefsMapper])
    conf setJobName "wiki parser"
    conf setMapperClass classOf[WikipediaToItemPrefsMapper]
    conf setReducerClass classOf[WikipediaToUserVectorReducer]

    conf setInputFormat classOf[TextInputFormat]

    conf setOutputFormat classOf[TextOutputFormat[VarLongWritable, VectorWritable]]
    conf setOutputKeyClass classOf[VarLongWritable]
    conf setOutputValueClass classOf[VarLongWritable]

    //    conf setJar "hadoop.jar"
    conf setCompressMapOutput true

    FileInputFormat setInputPaths(conf, inputPath)
    FileOutputFormat setOutputPath(conf, namePath)

    JobClient runJob conf


    val fileLinesTest = io.Source.fromFile(BASE_PHATH+"output_test_level1.txt").getLines.toList
    val fileLinesOutput = io.Source.fromFile(namePath + "/part-00000").getLines.toList
    val outputTest = fileLinesTest.reduce(_ + _)
    val output = fileLinesOutput.reduce(_ + _)
    println(outputTest.equals(output))
    outputTest should be equals output
  }
}
