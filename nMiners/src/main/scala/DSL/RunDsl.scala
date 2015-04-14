package DSL

import DSL.job.Implicits._
import DSL.job.JobUtils._
import DSL.job._

object RunDsl extends App {
  parse_data on "src/test/data/data_2/input_test_level1.txt" then in_parallel(produce(coocurrence_matrix as
    "coocurrence") and produce(user_vector as "user_vectors")) then
    multiply("coocurrence" by "user_vectors") then
    produce(recommendation as "recs") write_on "output.dat" then execute
}