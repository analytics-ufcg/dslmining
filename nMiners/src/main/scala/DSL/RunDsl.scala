package DSL

import dsl.job.Implicits._
import dsl.job.JobUtils._
import dsl.job._

object RunDsl extends App {
  parse_data on "src/test/data/data_2/input_test_level1.txt" in (5 nodes) then
    in_parallel(produce(coocurrence_matrix as "coocurrence") and produce(user_vector as "user_vectors")) in (6 nodes) then
    multiply("coocurrence" by "user_vectors") in (3 nodes) then
    produce(recommendation as "recs") in (5 nodes) write_on "output.dat" then execute
}