import dsl.job.Implicits._
import dsl.job.JobUtils._
import dsl.job._

object Main extends App {

    val dataset = "data/input.dat"
    val output = "src/main/resources/output2.dat"

//    val dataset = "/home/andryw/Projects/dslmining/nMiners/data/input.dat"
//     val output = "/home/andryw/OUT_TEST"
//    val dataset = args(0)
//    val output = args(1)

    println("rodando")
    parse_data on dataset then
      produce(user_vectors as "user_vector") then
      produce(similarity_matrix) then
      multiply("user_vector" by "similarity_matrix") then
      produce(recommendation) write_on output then execute

    println("terminei")
}