import dsl.job.JobUtils._
import dsl.job._
import dsl.job.Implicits._

/**
 * Created by arthur on 06/04/15.
 */
object Main {

  def main(args: Array[String]): Unit = {

    val dataset = "data/input.dat"
    val output = "data/output"

    //    val outputType = "TextOutputFormat";
    //
    //    val args = Array("--input", "data/input.dat","--output", "data/output","--booleanData","true","-s","SIMILARITY_COSINE", "--outputType", outputType)
    //    val recommender = new RecommenderJob()
    //    val prepPath: Path = new Path("temp/preparePreferenceMatrix/")
    //    val numberOfUsers = recommender.uservector(args)
    //val similarity = recommender.rowSimilarity(args, prepPath, 10)
    //val multiply = recommender.multiplication(args,prepPath)
    //val recommend = recommender.recommender(args, prepPath)

    parse_data on dataset then
      produce(user_vectors as "user_vector") then
      produce(similarity_matrix using COOCURRENCE as "coocurrence") then
      multiply("coocurrence" by "user_vector") then
      produce(recommendation) write_on output then execute
  }
}