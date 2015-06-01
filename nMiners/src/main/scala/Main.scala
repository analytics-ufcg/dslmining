import api._
import dsl.job.JobUtils._
import dsl.job.Implicits._
import dsl.job._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat, TextOutputFormat}
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable
import org.apache.mahout.cf.taste.hadoop.item.{VectorAndPrefsWritable, VectorOrPrefWritable}
import org.apache.mahout.common.AbstractJob
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, VectorWritable}
import utils.MapReduceUtils

/**
 * Created by arthur on 06/04/15.
 */
object Main {

  def main(args: Array[String]): Unit = {

    //val dataset = args(0)
    //val output = args(1)

    val a = Array("--input", "data/input.dat","--output", "data/output","--booleanData","true","-s","SIMILARITY_COSINE")
    val recommender = new RecommenderJob()
    val prepPath: Path = new Path("temp/preparePreferenceMatrix/")
    val numberOfUsers = recommender.uservector(a, prepPath);
    val similarity = recommender.rowSimilarity(a, prepPath, 10)
    val multiply = recommender.multiplication(a,prepPath)
    val recommend = recommender.recommender(a, prepPath)

//    parse_data on dataset then
//      produce(user_vectors) then
//      produce(similarity_matrix using COOCURRENCE as "coocurrence") then
//      multiply("coocurrence" by "user_vector") then
//     produce(recommendation) write_on output then execute

  }



}
