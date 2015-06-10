package nMinersTest

import api._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.mahout.math.{VarLongWritable, VectorWritable}
import org.scalatest.{FlatSpec, Matchers}

class RowSimilarityTest extends FlatSpec with Matchers{

  val BASE_PHATH = "src/test/resources/"
  //val BASE_PHATH_OUTPUT = BASE_PHATH + "output_similarity1/"
  //val BASE_PHATH_SIM_OUTPUT = BASE_PHATH_OUTPUT + "matrix/"

  "Similarity_Matrix" should "calculate similarity_matrix" in {
    val outputType = "TextOutputForm";

    val args = Array("--input", "data/input.dat","--output", "data/output","--booleanData","true","-s","SIMILARITY_COSINE", "--outputType", outputType)
    val prepPath: String = "temp/preparePreferenceMatrix/"
    val recommender = new RecommenderJob(prepPath)
    val numberOfUsers = recommender.uservector(args)
    val similarity = recommender.rowSimilarity(args, 10)

    val fileLinesTest = io.Source.fromFile(BASE_PHATH+ "Similarity/output_similarity").getLines.toList
    val fileLinesOutput = io.Source.fromFile("temp/preparePreferenceMatrix/similarityMatrix/part-r-00000").getLines.toList
    //val outputTest = fileLinesTest.reduce(_ + _)
    //val output = fileLinesOutput.reduce(_ + _)

    //outputTest should equal (output)
  }

 /* "Similarity_Matrix_EUCLIDEAN_DISTANCE" should "calculate similarity_matrix euclidean distance" in {
    val inputPath = BASE_PHATH+ "data_2_SIMILARITY_EUCLIDEAN_DISTANCE/input_test_level1.txt"
    val uservector = BASE_PHATH_OUTPUT+"user_vector/"
    val uservectorFile = uservector + "part-r-00000"
    val similarity = BASE_PHATH_SIM_OUTPUT+"similarity_matrix/"


    UserVectorGenerator.runJob(inputPath,uservector, classOf[TextInputFormat],
      classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]],true,None)

    SimilarityMatrix.generateSimilarityMatrix(
      uservectorFile,
      similarity,
      classOf[SequenceFileInputFormat[VarLongWritable,VectorWritable]],
      classOf[TextOutputFormat[IntWritable,VectorWritable]],true,similarityClassnameArg = "SIMILARITY_EUCLIDEAN_DISTANCE",basePath = BASE_PHATH_SIM_OUTPUT, numReduceTasks = None)


    val fileLinesTest = io.Source.fromFile(similarity+"/part-r-00000").getLines.toList
    val fileLinesOutput = io.Source.fromFile(BASE_PHATH + "data_2_SIMILARITY_EUCLIDEAN_DISTANCE/outputTest").getLines.toList
    val outputTest = fileLinesTest.reduce(_ + _)
    val output = fileLinesOutput.reduce(_ + _)

    outputTest should equal (output)
  }

  "Similarity_Matrix_SIMILARITY_COOCCURRENCE" should "calculate similarity_matrix similarity cooccurrence" in {
    val inputPath = BASE_PHATH+ "data_2_SIMILARITY_COOCCURRENCE/input_test_level1.txt"
    val uservector = BASE_PHATH_OUTPUT+"user_vector/"
    val uservectorFile = uservector + "part-r-00000"
    val similarity = BASE_PHATH_SIM_OUTPUT+"similarity_matrix/"


    UserVectorGenerator.runJob(inputPath,uservector, classOf[TextInputFormat],
      classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]],true,None)

    SimilarityMatrix.generateSimilarityMatrix(
      uservectorFile,
      similarity,
      classOf[SequenceFileInputFormat[VarLongWritable,VectorWritable]],
      classOf[TextOutputFormat[IntWritable,VectorWritable]],true,similarityClassnameArg = "SIMILARITY_COOCCURRENCE",basePath = BASE_PHATH_SIM_OUTPUT, numReduceTasks = None)


    val fileLinesTest = io.Source.fromFile(similarity+"/part-r-00000").getLines.toList
    val fileLinesOutput = io.Source.fromFile(BASE_PHATH + "data_2_SIMILARITY_COOCCURRENCE/outputTest").getLines.toList
    val outputTest = fileLinesTest.reduce(_ + _)
    val output = fileLinesOutput.reduce(_ + _)

    outputTest should equal (output)
  }


  "Similarity_Matrix_SIMILARITY_COSINE" should "calculate similarity_matrix similarity cosine" in {
    val inputPath = BASE_PHATH+ "data_2_SIMILARITY_COSINE/input_test_level1.txt"
    val uservector = BASE_PHATH_OUTPUT+"user_vector/"
    val uservectorFile = uservector + "part-r-00000"
    val similarity = BASE_PHATH_SIM_OUTPUT+"similarity_matrix/"


    UserVectorGenerator.runJob(inputPath,uservector, classOf[TextInputFormat],
      classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]],true,None)

    SimilarityMatrix.generateSimilarityMatrix(
      uservectorFile,
      similarity,
      classOf[SequenceFileInputFormat[VarLongWritable,VectorWritable]],
      classOf[TextOutputFormat[IntWritable,VectorWritable]],true,similarityClassnameArg = "SIMILARITY_COSINE",basePath = BASE_PHATH_SIM_OUTPUT, numReduceTasks = None)


    val fileLinesTest = io.Source.fromFile(similarity+"/part-r-00000").getLines.toList
    val fileLinesOutput = io.Source.fromFile(BASE_PHATH + "data_2_SIMILARITY_COSINE/outputTest").getLines.toList
    val outputTest = fileLinesTest.reduce(_ + _)
    val output = fileLinesOutput.reduce(_ + _)

    outputTest should equal (output)
  }*/
}
