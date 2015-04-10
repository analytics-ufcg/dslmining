import java.util.Iterator

import Utils.{MapReduceUtils, Implicits}
import Implicits._
import API._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, SequenceFileInputFormat, MultipleInputs}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable
import org.apache.mahout.cf.taste.hadoop.item.{VectorAndPrefsWritable, SimilarityMatrixRowWrapperMapper, VectorOrPrefWritable}
import org.apache.mahout.cf.taste.hadoop.preparation.PreparePreferenceMatrixJob
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.CooccurrenceCountSimilarity
import org.apache.mahout.math.{VarIntWritable, VectorWritable, VarLongWritable}

/**
 * Created by arthur on 06/04/15.
 */
object Main {

  def generateMap() = {
    val inputPath = "src/test/data/input_test_level1.txt"
    val outPutPath = "src/outputMap"

    MapReduceUtils.runMap("First Phase",classOf[WikipediaToItemPrefsMapper],
      classOf[VarLongWritable],classOf[VarLongWritable],
      classOf[TextInputFormat],classOf[TextOutputFormat[VarLongWritable, VectorWritable]],inputPath,outPutPath,true)
  }

  def main(args: Array[String]): Unit = {
    //generateMap()
  //  generateUserVectors()
    //coocurrence()
   // prepare()
    multiply()
  }

  def generateUserVectors() = {
    val inputPath = "src/test/data/input_test_level1.txt"
    val outPutPath = "src/output"

    MapReduceUtils.runJob("First Phase",classOf[WikipediaToItemPrefsMapper],classOf[WikipediaToUserVectorReducer],
      classOf[VarLongWritable],classOf[VarLongWritable],classOf[VarLongWritable],classOf[VectorWritable],
      classOf[TextInputFormat],classOf[SequenceFileOutputFormat[VarLongWritable, VectorWritable]],inputPath,outPutPath,true)


  }

  def coocurrence() = {
    val inputPath = "src/output/part-r-00000"
    val outPutPath = "src/output1"

    MapReduceUtils.runJob("First Phase",classOf[UserVectorToCooccurrenceMapper],classOf[UserVectorToCooccurenceReduce],
      classOf[VarIntWritable],classOf[VarIntWritable],classOf[VarIntWritable],classOf[VectorWritable],
      classOf[SequenceFileInputFormat[VarLongWritable, VectorWritable]],classOf[SequenceFileOutputFormat[VarIntWritable,VectorWritable]],inputPath,outPutPath,true)
  }


  def prepare() = {

    val inputPath1 = "src/test/data/input_test_level3_2.dat"
    val inputPath2 = "src/test/data/input_test_level3_1.dat"

    val outPutPath = "src/output2"

    MapReduceUtils.run2MappersJob("Prepare",classOf[CooccurrenceColumnWrapperMapper],classOf[UserVectorSplitterMapper],   classOf[ToVectorAndPrefReducer],
      mapOutputKeyClass = classOf[VarIntWritable],mapOutputValueClass = classOf[VectorOrPrefWritable],
      classOf[VarIntWritable], classOf[VectorAndPrefsWritable],
      classOf[SequenceFileInputFormat[VarIntWritable,VectorWritable]],classOf[SequenceFileInputFormat[VarLongWritable,VectorWritable]],
      classOf[SequenceFileOutputFormat[VarIntWritable,VectorAndPrefsWritable]],inputPath1,inputPath2,outPutPath,true)

  }

  def multiply() = {

    val inputPath = "src/test/data/input_test_level4.dat"
    val outPutPath = "src/output3"

    val job = MapReduceUtils.prepareJob("Prepare",classOf[PartialMultiplyMapper],classOf[AggregateAndRecommendReducer],
      classOf[VarLongWritable], classOf[VectorWritable],
      classOf[VarLongWritable], classOf[RecommendedItemsWritable],
      classOf[SequenceFileInputFormat[VarIntWritable,VectorAndPrefsWritable]],
      classOf[TextOutputFormat[VarLongWritable,RecommendedItemsWritable]],inputPath,outPutPath)

    var conf : Configuration = job getConfiguration ()
    conf.set(AggregateAndRecommendReducer.ITEMID_INDEX_PATH,"")
    conf.setInt(AggregateAndRecommendReducer.NUM_RECOMMENDATIONS, 10)

    println(job.getConfiguration().getInt(AggregateAndRecommendReducer.NUM_RECOMMENDATIONS,1))
    MapReduceUtils.deleteFolder(outPutPath,conf)
    job.waitForCompletion(true)
  }


}
