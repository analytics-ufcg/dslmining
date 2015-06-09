import api.RecommenderJob

/**
 * Created by arthur on 06/04/15.
 */
object Main {

  def main(args: Array[String]): Unit = {

    //val dataset = args(0)
    //val output = args(1)

    val outputType = "TextOutputForm";

    val args = Array("--input", "data/input.dat","--output", "data/output","--booleanData","true","-s","SIMILARITY_COSINE", "--outputType", outputType)
    val prepPath: String = "temp/preparePreferenceMatrix/"
    val recommender = new RecommenderJob(prepPath)
    val numberOfUsers = recommender.uservector(args)
    val similarity = recommender.rowSimilarity(args, 10)
    //val multiply = recommender.multiplication(args,prepPath)
    //val recommend = recommender.recommender(args, prepPath)

//    parse_data on dataset then
//      produce(user_vectors) then
//      produce(similarity_matrix using COOCURRENCE as "coocurrence") then
//      multiply("coocurrence" by "user_vector") then
//     produce(recommendation) write_on output then execute

  }
}