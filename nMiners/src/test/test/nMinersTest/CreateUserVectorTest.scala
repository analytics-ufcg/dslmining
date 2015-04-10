package nMinersTest

import API.{UserVectorGenerator, WikipediaToUserVectorReducer, WikipediaToItemPrefsMapper}
import Utils.MapReduceUtils

//import Utils._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
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

    UserVectorGenerator.runJob(inputPath,namePath, classOf[TextInputFormat],
      classOf[TextOutputFormat[VarLongWritable, VectorWritable]],true)

    val fileLinesTest = io.Source.fromFile(BASE_PHATH+"output_test_level1.txt").getLines.toList
    val fileLinesOutput = io.Source.fromFile(namePath + "/part-r-00000").getLines.toList
    val outputTest = fileLinesTest.reduce(_ + _)
    val output = fileLinesOutput.reduce(_ + _)



    println(outputTest.equals(output))
    outputTest should equal (output)
  }
}