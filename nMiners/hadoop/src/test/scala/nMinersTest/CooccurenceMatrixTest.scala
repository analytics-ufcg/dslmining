//package nMinersTest
//
//import hadoop.api_hadoop._
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
//import org.apache.mahout.math.{VarLongWritable, VectorWritable}
//import org.scalatest.{FlatSpec, Matchers}
//
//class CooccurenceMatrixTest extends FlatSpec with Matchers{
//
//
//  val BASE_PHATH = "src/test/resources/"
//
//  "Level one" should "execute first mapreduce" in {
//    val inputPath = BASE_PHATH+"data_1/input_test_level2"
//    val dirOutputName = BASE_PHATH+"output_test_level2"; // Path da pasta e nao do arquivo
//
//    MatrixGenerator.runJob(inputPath,dirOutputName,classOf[SequenceFileInputFormat[VarLongWritable, VectorWritable]],
//      classOf[TextOutputFormat[VarLongWritable, VectorWritable]],true,None)
//
//    val fileLinesTest = io.Source.fromFile(BASE_PHATH+"data_1/output_test_level2.txt").getLines.toList
//    val fileLinesOutput = io.Source.fromFile(dirOutputName + "/part-r-00000").getLines.toList
//    val outputTest = fileLinesTest.reduce(_ + _)
//    val output = fileLinesOutput.reduce(_ + _)
//
//    outputTest should equal (output)
//  }
//}
