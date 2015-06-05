package nMinersTest

import api.RecommenderJob
import org.apache.hadoop.fs.Path

//import Utils._

import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by leonardo on 08/04/15.
 */
class CreateUserVectorTest extends FlatSpec with Matchers{
  val BASE_PHATH = "src/test/resources/"
  "Level one" should "execute first mapreduce" in {
    val inputPath = BASE_PHATH+"data_1/input_test_level1.txt"
    val nameOutputPath = BASE_PHATH+"output_test_level1"; // Path da pasta e nao do arquivo

//    UserVectorGenerator.runJob(inputPath,nameOutputPath, classOf[TextInputFormat],
//      classOf[TextOutputFormat[VarLongWritable, VectorWritable]],true,None)

    val args = Array("--input", "data/input.dat","--output", nameOutputPath,"--booleanData","true","-s","SIMILARITY_COSINE")
    val recommender = new RecommenderJob()
    val prepPath: Path = new Path("temp/preparePreferenceMatrix/")
    val numberOfUsers = recommender.uservector(args, prepPath)

    val fileLinesTest = io.Source.fromFile(BASE_PHATH+"data_1/output_test_level1.txt").getLines.toList
    val fileLinesOutput = io.Source.fromFile(nameOutputPath + "/part-r-00000").getLines.toList
    val outputTest = fileLinesTest.reduce(_ + _)
    val output = fileLinesOutput.reduce(_ + _)

    outputTest should equal (output)
  }
}
