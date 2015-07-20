package nMinersTest.api_spark_test

import java.nio.file.{Files, Paths}

import api_hadoop.SimilarityMatrix
import api_spark.UserVectorDriver
import org.apache.mahout.drivers.ItemSimilarityDriver._
import org.apache.mahout.math.indexeddataset.Schema
import org.scalatest.{FlatSpec, Matchers}
import utils.Writer

/**
 * Created by arthur on 30/06/15.
 */
class UserVectorDriverTest  extends FlatSpec with Matchers{

    "UserVectorDriver" should "show mc is null" in {

      val InFile = "data/actions.csv" //Input Data
      val OutPath = Some("data/similarity-matrices/") // Output path where the matrix should be after the execution

      intercept[IllegalArgumentException] {
        val userVectorDrm = UserVectorDriver.run(Array(
          "--input", InFile,
          "--output", OutPath.getOrElse(""),
          "--master", "local"
        ))
      }

    }


  "UserVectorDriver" should "run" in {

       val InFile = "data/actions.csv" //Input Data
       val OutPath = Some("data/similarity-matrices/") // Output path where the matrix should be after the execution

       UserVectorDriver.start()
       val userVectorDrm = UserVectorDriver.run(Array(
         "--input", InFile,
         "--output", OutPath.getOrElse(""),
         "--master", "local"
       ))

       println(userVectorDrm(0).collect)

       UserVectorDriver.stop()

     }



      it should "show exception because is stopped" in {

        val InFile = "data/actions.csv" //Input Data
        val OutPath = Some("data/similarity-matrices/") // Output path where the matrix should be after the execution

        UserVectorDriver.start()

        val userVectorDrm = UserVectorDriver.run(Array(
          "--input", InFile,
          "--output", OutPath.getOrElse(""),
          "--master", "local"
        ))

        print(userVectorDrm(0).collect)

        UserVectorDriver.stop()

        intercept[IllegalStateException] {
          val userVectorDrma = UserVectorDriver.run(Array(
            "--input", InFile,
            "--output", OutPath.getOrElse(""),
            "--master", "local"
          ))
        }


      }

    it should "save a DRM" in {

      val InFile = "data/actions.csv" //Input Data
      val OutPath = Some("data/similarity-matrices/") // Output path where the matrix should be after the execution

      UserVectorDriver.start()

      val userVectorDrm = UserVectorDriver.run(Array(
        "--input", InFile,
        "--output", OutPath.getOrElse(""),
        "--master", "local"
      ))

      print(userVectorDrm(0).collect)

      Writer.context = UserVectorDriver.getContext()
      Writer.indexedDataset = UserVectorDriver.indexedDataset
      Writer.writeSchema = UserVectorDriver.writeSchema

      Writer.writeDRM(userVectorDrm(0),"src/test/resources/UserVectors")


      UserVectorDriver.stop()
    }
}
