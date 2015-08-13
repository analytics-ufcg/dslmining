import dsl_spark.job.JobUtils._
import dsl_spark.job._
import dsl_spark.job.Implicits._

object Main {

  def main(args: Array[String]): Unit = {

    val dataset = "/home/arthur/dslmining_Temp/dslmining/nMiners/data/input.dat"
    val output = "data/out"



//    val dataset = args(0)
//    val output = args(1)

    parse_data on dataset then
      produce(user_vectors as "user_vector") then
      produce(similarity_matrix as "coocurrence") then execute
      multiply("user_vector" by "coocurrence") then
      produce(recommendation) write_on output then execute


//    parse_data on dataset then
//      dsl_spark.job.JobUtils.produce(user_vectors as "matrix1") then
//      dsl_spark.job.JobUtils.produce(similarity_matrix as "matrix2")  then
//      multiply("matrix1" by "matrix2") then
//      produce(recommendation) write_on output then dsl_spark.job.execute

  }
}