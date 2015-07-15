package nMinersTest.api_spark_test

//import api_spark.SimilarityMatrix
import org.scalatest.{FlatSpec, Matchers}
import java.nio.file.{Paths, Files}
import java.nio.file.{Paths, Files}

/**
 * Created by arthur on 30/06/15.
 */
class ApiSparkTest  extends FlatSpec with Matchers{

     it should " create output folder with the correct results" in {

       val InFile = "data/actions.csv"
       val OutPath = Some("data/similarity-matrices/")

      // SimilarityMatrix.run(InFile,OutPath,"local")

       Files.exists(Paths.get(OutPath.getOrElse(""))) should be equals(true)

      val correctOutput =  scala.io.Source.fromFile("data/Correct_Output_apiSpark").mkString
      val myOutput = scala.io.Source.fromFile(OutPath.getOrElse("") + "similarity-matrix/part-00000").mkString

      correctOutput equals myOutput should be equals true
     }

      var InFile = "data/actions.csv"
      var OutPath = Some("")

      it should "throw an exception if an empty string is passed as ouput path parameter" in {
        a [Exception] should be thrownBy {
        //  SimilarityMatrix.run(InFile,Some(""),"local")
        }
      }

       InFile = ""
       OutPath = Some("data/similarity-matrices/")

         Files.exists(Paths.get(OutPath.getOrElse(""))) should be equals(true)

        it should "throw an exception if an empty string is passed as input path parameter" in {
           a [Exception] should be thrownBy {
//             SimilarityMatrix.run(InFile,OutPath,"local")
           }
       }
}
