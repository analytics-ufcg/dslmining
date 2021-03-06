//package nMinersTest
//
//import hadoop.api_hadoop.RecommenderJob
//import org.apache.hadoop.fs.Path
//
////import Utils._
//
//import org.scalatest.{FlatSpec, Matchers}
//
///**
// * Created by leonardo on 08/04/15.
// */
//class UserVectorTest extends FlatSpec with Matchers{
//  val BASE_PHATH = "src/test/resources/"
//
//  "Level one" should "execute user vector correctly" in {
//    val inputPath = BASE_PHATH+ "data_1/input_test_user_vector.txt"
//    val nameOutputPath = BASE_PHATH+"output_test_level1";
//    val outputType = "TextOutputFormat";
//
//    val args = Array("--input", inputPath,"--output", nameOutputPath,"--booleanData","true","-s","SIMILARITY_COSINE", "--outputType", outputType)
//    val prepPath: String = nameOutputPath + "/temp/preparePreferenceMatrix/"
//    val recommender = new RecommenderJob(prepPath)
//    val numberOfUsers = recommender.uservector(args)
//
//    val fileLinesTest = io.Source.fromFile(BASE_PHATH+ "data_1/output_test_user_vector.txt").getLines.toList
//    val fileLinesOutput = io.Source.fromFile(nameOutputPath + "/temp/preparePreferenceMatrix/userVectors/part-r-00000").getLines.toList
//    val outputTest = fileLinesTest.reduce(_ + _)
//    val output = fileLinesOutput.reduce(_ + _)
//
//    outputTest should equal (output)
//  }
//}
