package nMinersTest.api_spark_test

import java.nio.file.{Files, Paths}

import api_hadoop.SimilarityMatrix
import api_spark.UserVectorDriver
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by arthur on 30/06/15.
 */
class UserVectorDriverTest  extends FlatSpec with Matchers{

     "UserVectorDriver" should "run" in {

       val InFile = "data/actions.csv" //Input Data
       val OutPath = Some("data/similarity-matrices/") // Output path where the matrix should be after the execution

       val userVectorDrm = UserVectorDriver.run(Array(
         "--input", InFile,
         "--output", OutPath.getOrElse(""),
         "--master", "local"
       ))

       print(userVectorDrm(0).collect)

     }
}
