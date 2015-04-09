package nMinersTest

import Utils._
import API.{UserVectorToCooccurenceReduce, UserVectorToCooccurrenceMapper, WikipediaToItemPrefsMapper, WikipediaToUserVectorReducer}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, VectorWritable}
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
    val inputPath = BASE_PHATH+"input_test_level2"
    val dirOutputName = BASE_PHATH+"output_test_level2"; // Path da pasta e nao do arquivo

    MapReduceUtils.runJob(
      "Second Phase",
      classOf[WikipediaToItemPrefsMapper],
      classOf[UserVectorToCooccurenceReduce],
      classOf[VarIntWritable],
      classOf[VarIntWritable],
      classOf[VarLongWritable],
      classOf[VectorWritable],
      classOf[SequenceFileInputFormat[VarLongWritable, VectorWritable]],
      classOf[TextOutputFormat[VarLongWritable, VectorWritable]],
      inputPath,
      dirOutputName,
      true)


    /*
      val conf = new JobConf(classOf[WikipediaToItemPrefsMapper])
      conf setJobName "wiki parser"

      conf setOutputKeyClass classOf[VarLongWritable]
      conf setOutputValueClass classOf[VectorWritable]

      conf setMapOutputKeyClass classOf[VarIntWritable]
      conf setMapOutputValueClass classOf[VarIntWritable]

      conf setMapperClass classOf[UserVectorToCooccurrenceMapper]
      conf setReducerClass classOf[UserVectorToCooccurenceReduce]

      conf setInputFormat classOf[SequenceFileInputFormat[VarLongWritable, VectorWritable]]
      conf setOutputFormat classOf[TextOutputFormat[VarLongWritable, VectorWritable]]

      conf setCompressMapOutput true

      */

    FileInputFormat setInputPaths(conf, inputPath)
    FileOutputFormat setOutputPath(conf, dirOutputName)

    //Delete the output path before run, to avoid exception
    val fs1: FileSystem = FileSystem.get(conf);
    val out1 = dirOutputName;
    fs1.delete(out1, true);

    JobClient runJob conf


    val fileLinesTest = io.Source.fromFile(BASE_PHATH+"output_test_level2.txt").getLines.toList
    val fileLinesOutput = io.Source.fromFile(dirOutputName + "/part-00000").getLines.toList
    val outputTest = fileLinesTest.reduce(_ + _)
    val output = fileLinesOutput.reduce(_ + _)



    println(outputTest.equals(output))
    outputTest should equal (output)
  }
}
