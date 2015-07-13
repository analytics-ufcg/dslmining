package nMinersTest.api_spark_test

import java.nio.file.{Files, Paths}

import api_spark.SimilarityMatrixSpark
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by arthur on 30/06/15.
 */
class ApiSparkTest  extends FlatSpec with Matchers{

     it should " create output folder with the correct results" in {

       val InFile = "data/actions.csv"
       val OutPath = Some("data/similarity-matrices/")

       SimilarityMatrixSpark.run(InFile,OutPath,"local")

       Files.exists(Paths.get(OutPath.getOrElse(""))) should be equals(true)

      val correctOutput =  scala.io.Source.fromFile("data/Correct_Output_apiSpark").mkString
      val myOutput = scala.io.Source.fromFile(OutPath.getOrElse("") + "similarity-matrix/part-00000").mkString

      correctOutput equals myOutput should be equals true
     }

      var InFile = "data/actions.csv"
      var OutPath = Some("")

      it should "throw an exception if an empty string is passed as ouput path parameter" in {
        a [Exception] should be thrownBy {
          SimilarityMatrixSpark.run(InFile,Some(""),"local")
        }
      }

       InFile = ""
       OutPath = Some("data/similarity-matrices/")

         Files.exists(Paths.get(OutPath.getOrElse(""))) should be equals(true)

        it should "throw an exception if an empty string is passed as input path parameter" in {
           a [Exception] should be thrownBy {
             SimilarityMatrixSpark.run(InFile,OutPath,"local")
           }
       }
}
