package nMinersTest

import api.{PrepareMatrixGenerator}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable
import org.apache.mahout.math.{VarIntWritable, VectorWritable}
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by arthur on 10/04/15.
 */
class PrepareMatrixTest  extends FlatSpec with Matchers{

  val BASE_PHATH = "src/test/resources/"
  "Level Three" should "execute three mapreduce" in {

    val inputPath = BASE_PHATH+"data_1/input_test_level3_2.dat"
    val inputPath2 = BASE_PHATH+"data_1/input_test_level3_1.dat"

    val namePath = BASE_PHATH+"output_3"; // Path da pasta e nao do arquivo

    PrepareMatrixGenerator.runJob(inputPath,inputPath2,namePath,classOf[SequenceFileInputFormat[VarIntWritable,VectorWritable]],
      classOf[TextOutputFormat[VarIntWritable,VectorAndPrefsWritable]],true)

    val fileLinesTest = io.Source.fromFile(BASE_PHATH+"data_1/output_test_level3.txt").getLines.toList
    val fileLinesOutput = io.Source.fromFile(namePath + "/part-r-00000").getLines.toList
    val outputTest = fileLinesTest.reduce(_ + _)
    val output = fileLinesOutput.reduce(_ + _)

    outputTest should equal (output)
  }
}
