package nMinersTest

import api._
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat}
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, VectorWritable}
import org.scalatest.{FlatSpec, Matchers}

class SimlarityMatrixTest extends FlatSpec with Matchers{
  //TODO DOCUMENTATION AND REMOVE ALL COMMENTS

  val BASE_PHATH = "src/test/resources/"
  val BASE_PHATH_OUTPUT = BASE_PHATH + "output_similarity1/"
  val BASE_PHATH_SIM_OUTPUT = BASE_PHATH_OUTPUT + "matrix/"

  "Similarity_Matrix" should "calculate similarity_matrix" in {
    val inputPath = BASE_PHATH+"data_2/input_test_level1.txt"
    val uservector = BASE_PHATH_OUTPUT+"user_vector/";
    val uservectorFile = uservector + "part-r-00000";
    val ratingMatrix = BASE_PHATH_SIM_OUTPUT+"rating_matrix/";
    val similarity = BASE_PHATH_SIM_OUTPUT+"similarity_matrix/";

    val similarityFile = similarity + "part-r-00000";
    val pathOutputMatrix = BASE_PHATH_SIM_OUTPUT + "/data_prepare"

    UserVectorGenerator.runJob(inputPath,uservector, classOf[TextInputFormat],
      classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]],true,None)

    SimilarityMatrix.generateSimilarityMatrix(
      uservectorFile,
      similarity,
      classOf[SequenceFileInputFormat[VarLongWritable,VectorWritable]],
      classOf[SequenceFileOutputFormat[IntWritable,VectorWritable]],true,similarityClassnameArg = "SIMILARITY_COOCCURRENCE",basePath = BASE_PHATH_SIM_OUTPUT, numReduceTasks = None)

    PrepareMatrixGenerator.runJob(inputPath1 = similarityFile, inputPath2 = uservectorFile, outPutPath = pathOutputMatrix,
      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VectorWritable]],
      outputFormatClass = classOf[SequenceFileOutputFormat[VarIntWritable, VectorAndPrefsWritable]],
      deleteFolder = true)



  }

  //TODO TESTS TO COMPARE MAHOUT OUTPUT AND nMINERS OUTPUT
}
