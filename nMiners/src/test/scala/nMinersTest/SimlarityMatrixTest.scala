package nMinersTest

import api._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, VectorWritable}
import org.scalatest.{FlatSpec, Matchers}
import utils.MapReduceUtils

class SimlarityMatrixTest extends FlatSpec with Matchers{
//TODO DOCUMENTATION AND REMOVE ALL COMMENTS

  val BASE_PHATH = "src/test/resources/"
  val BASE_PHATH_OUTPUT = BASE_PHATH + "output_similarity1/"

  "Similarity_Matrix" should "calculate similarity_matrix" in {
    val inputPath = BASE_PHATH+"data_2/input_test_level1.txt"
    val dirOutputName = BASE_PHATH_OUTPUT+"similarity/"; // Path da pasta e nao do arquivo
    val uservector = BASE_PHATH_OUTPUT+"user_vector/"; // Path da pasta e nao do arquivo
    val uservectorFile = uservector + "part-r-00000"; // Path da pasta e nao do arquivo
    val ratingMatrix = BASE_PHATH_OUTPUT+"rating_matrix/"; // Path da pasta e nao do arquivo
    val ratingMatrixFile = BASE_PHATH_OUTPUT+"rating_matrix/"+ "part-r-00000"; // Path da pasta e nao do arquivo
    val similarityFile = BASE_PHATH_OUTPUT+"phase2/"+ "part-r-00000"; // Path da pasta e nao do arquivo

    UserVectorGenerator.runJob(inputPath,uservector, classOf[TextInputFormat],
      classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]],true,None)


  // TODO MERGE THE METHODS runToItemJob, runCountObservationsJob, runJob
    RowSimilarityJobAnalytics.runToItemJob(
      uservectorFile,
      ratingMatrix,
      classOf[SequenceFileInputFormat[VarLongWritable,VectorWritable]],
      classOf[SequenceFileOutputFormat[IntWritable,VectorWritable]],true,BASE_PHATH_OUTPUT)


        RowSimilarityJobAnalytics.runCountObservationsJob(
          ratingMatrixFile,
          BASE_PHATH_OUTPUT + "phase1/",
            classOf[SequenceFileInputFormat[VarLongWritable,VectorWritable]],
        classOf[TextOutputFormat[IntWritable,VectorWritable]],true,BASE_PHATH_OUTPUT)

    RowSimilarityJobAnalytics.runJob(
      ratingMatrixFile,
      BASE_PHATH_OUTPUT + "phase2/",
      classOf[SequenceFileInputFormat[VarLongWritable,VectorWritable]],
      classOf[SequenceFileOutputFormat[IntWritable,VectorWritable]],true,similarityClassnameArg = "SIMILARITY_COOCCURRENCE",basePath = BASE_PHATH_OUTPUT)

    // TODO CREATE A NEW OBJECT TO RUN A MULTIPLY MATRIX

    val pathToOutput1 = BASE_PHATH_OUTPUT + "/data_prepare"
    PrepareMatrixGenerator.runJob(inputPath1 = similarityFile, inputPath2 = uservectorFile, outPutPath = pathToOutput1,
      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VectorWritable]],
      outputFormatClass = classOf[SequenceFileOutputFormat[VarIntWritable, VectorAndPrefsWritable]],
      deleteFolder = true)

    val pathToInput = pathToOutput1 + "/part-r-00000"
    val pathToOutput2 = BASE_PHATH_OUTPUT + "/data_multiplied"

    val job = MapReduceUtils.prepareJob(jobName = "Prepare", mapperClass = classOf[PartialMultiplyMapper],
      reducerClass = classOf[AggregateAndRecommendReducer], mapOutputKeyClass = classOf[VarLongWritable],
      mapOutputValueClass = classOf[VectorWritable],
      outputKeyClass = classOf[VarLongWritable], outputValueClass = classOf[RecommendedItemsWritable],
      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VectorAndPrefsWritable]],
      outputFormatClass = classOf[TextOutputFormat[VarLongWritable, RecommendedItemsWritable]],
      pathToInput, pathToOutput2)

    var conf: Configuration = job getConfiguration()
    conf.set(AggregateAndRecommendReducer.ITEMID_INDEX_PATH, "")
    conf.setInt(AggregateAndRecommendReducer.NUM_RECOMMENDATIONS, 10)

    MapReduceUtils.deleteFolder(pathToOutput2, conf)
    job.waitForCompletion(true)

    //
//    val fileLinesTest = io.Source.fromFile(BASE_PHATH+"data_1/output_test_level2.txt").getLines.toList
//    val fileLinesOutput = io.Source.fromFile(dirOutputName + "/part-r-00000").getLines.toList
//    val outputTest = fileLinesTest.reduce(_ + _)
//    val output = fileLinesOutput.reduce(_ + _)
//
//    outputTest should equal (output)
  }
//TODO TESTS TO COMPARE MAHOUT OUTPUT AND nMINERS OUTPUT
}
