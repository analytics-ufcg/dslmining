import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ToolRunner
import org.apache.mahout.common.AbstractJob

/**
 * Created by arthur on 06/04/15.
 */
object Main extends AbstractJob with App {
  //val dataset = args(0)
  //val output = args(1)

  ToolRunner.run(new Configuration(), this, args);

  // parse_data on dataset then
  // produce(user_vectors) then
  // produce(similarity_matrix using COOCURRENCE as "coocurrence") then
  // multiply("coocurrence" by "user_vector") then
  // produce(recommendation) write_on output then execute

  override def run(string: Array[String]) = {
    val arguments = if (string.isEmpty) {
      "--input data/input.dat --output data/output --booleanData true -s SIMILARITY_COSINE" split " "
    } else {
      args
    }

    val prepPath: Path = new Path("/tmp")
    val numberOfUsers = new RecommenderJob().uservector(arguments, prepPath);
    val similarity = new RecommenderJob().rowSimilarity(arguments, prepPath, 10)
    val multiply = new RecommenderJob().multiplication(arguments, prepPath)
    val recommend = new RecommenderJob().recommender(arguments, prepPath)

    0
  }
}