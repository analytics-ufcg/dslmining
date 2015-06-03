;

/**
 * Created by arthur on 06/04/15.
 */
object Main extends App {
  //val dataset = args(0)
  //val output = args(1)

  // parse_data on dataset then
  // produce(user_vectors) then
  // produce(similarity_matrix using COOCURRENCE as "coocurrence") then
  // multiply("coocurrence" by "user_vector") then
  // produce(recommendation) write_on output then execute

    //val dataset = args(0)
    //val output = args(1)

    val arguments = if(args.isEmpty) {
      "--input data/input.dat --output data/output --booleanData true -s SIMILARITY_COSINE" split " "
    } else {
      args
    }
    val numberOfUsers = new RecommenderJob().uservector(arguments);
    val similarity = new RecommenderJob().rowSimilarity(arguments, 10)
    val multiply = new RecommenderJob().multiplication(arguments)
    val recommend = new RecommenderJob().recommender(arguments)

//    parse_data on dataset then
//      produce(user_vectors) then
//      produce(similarity_matrix using COOCURRENCE as "coocurrence") then
//      multiply("coocurrence" by "user_vector") then
//     produce(recommendation) write_on output then execute

}