package nMinersTest

import API._
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.mahout.math.{VarLongWritable, VectorWritable}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by leonardo on 08/04/15.
 */
class CooccurenceMatrixTest extends FlatSpec with Matchers{

//  def cleanDataTrash() = {
//    return "Not implemented yet";
//  }


  val BASE_PHATH = "src/test/data/"
  "Level one" should "execute first mapreduce" in {
    val inputPath = BASE_PHATH+"data_1/input_test_level2"
    val dirOutputName = BASE_PHATH+"output_test_level2"; // Path da pasta e nao do arquivo

    MatrixGenerator.runJOb(inputPath,dirOutputName,classOf[SequenceFileInputFormat[VarLongWritable, VectorWritable]],
      classOf[TextOutputFormat[VarLongWritable, VectorWritable]],true)

    val fileLinesTest = io.Source.fromFile(BASE_PHATH+"data_1/output_test_level2.txt").getLines.toList
    val fileLinesOutput = io.Source.fromFile(dirOutputName + "/part-r-00000").getLines.toList
    val outputTest = fileLinesTest.reduce(_ + _)
    val output = fileLinesOutput.reduce(_ + _)

    outputTest should equal (output)
  }
}