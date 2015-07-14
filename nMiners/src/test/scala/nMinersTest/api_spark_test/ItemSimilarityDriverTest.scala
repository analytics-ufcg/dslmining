package nMinersTest.api_spark_test

import api_spark.{ItemSimilarityDriver, UserVectorDriver}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by arthur on 30/06/15.
 */
class ItemSimilarityDriverTest  extends FlatSpec with Matchers{

     "UserVectorDriver" should "run" in {

       val inputFile = "data/actions.csv" //Input Data
       val outPath = Some("data/similarity-matrices/") // Output path where the matrix should be after the execution
       val userVectorDrm = UserVectorDriver.run(Array(
           "--input", inputFile,
           "--output", outPath.getOrElse(""),
           "--master", "local"
         ))
       //The method below takes the correct parameters in order to call the Main from ItemSimilarity object
       print(userVectorDrm(0).collect) //The error is right HERE !!!!

       print("oi")


         val item = ItemSimilarityDriver.run(userVectorDrm, Array(
           "--input", inputFile,
           "--output", outPath.getOrElse(""),
           "--master", "local"
         ))


//       print(item.collect) //The error is right HERE !!!!

     }
}
