package DSL.job

import DSL.job.Utils._
import DSL.job.Implicits._

object main extends App {
  parse_data on "dataset.csv" then in_parallel(produce(coocurrence_matrix as "coocurrence") and
    produce(user_vector as "user_vectors")) then
    multiply("coocurrence" by "user_vectors") then
    produce(recommendation as "recs") write_on "output.dat" then execute

}