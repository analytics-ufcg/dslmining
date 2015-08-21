package nMinersTest.api_spark_test

import spark.api.{UserVectorDriver, ItemSimilarityDriver}

import org.scalatest.{FlatSpec, Matchers}
import utils.Writer

/**
 * Created by arthur on 30/06/15.
 */
class ItemSimilarityDriverTest  extends FlatSpec with Matchers{

     "ItemSimilarityDriver" should "run" in {

       val inputFile = "data/actions.csv" //Input Data
       val outPath = Some("data/similarity-matrices/") // Output path where the matrix should be after the execution

       UserVectorDriver.start()
       val userVectorDrm = UserVectorDriver.run(Array(
           "--input", inputFile,
           "--output", outPath.getOrElse(""),
           "--master", "local"
         ))
       //The method below takes the correct parameters in order to call the Main from ItemSimilarity object
       print(userVectorDrm(0).collect)

         val item = ItemSimilarityDriver.run(userVectorDrm, Array(
           "--input", inputFile,
           "--output", outPath.getOrElse(""),
           "--master", "local"
         ))(UserVectorDriver.getParser(),UserVectorDriver.getContext(),UserVectorDriver.indexedDataset)

       print(item(0).collect)


       UserVectorDriver.stop()

     }

    it should "save a DRM" in {

      val inputFile = "data/actions.csv" //Input Data
      val outPath = Some("data/similarity-matrices/") // Output path where the matrix should be after the execution

      UserVectorDriver.start()
      val userVectorDrm = UserVectorDriver.run(Array(
        "--input", inputFile,
        "--output", outPath.getOrElse(""),
        "--master", "local"
      ))
      //The method below takes the correct parameters in order to call the Main from ItemSimilarity object
      print(userVectorDrm(0).collect)

      Writer.context = UserVectorDriver.getContext()
      Writer.indexedDataset = UserVectorDriver.indexedDataset
      Writer.writeSchema = UserVectorDriver.writeSchema

      val item = ItemSimilarityDriver.run(userVectorDrm, Array(
        "--input", inputFile,
        "--output", outPath.getOrElse(""),
        "--master", "local"
      ))(UserVectorDriver.getParser(),UserVectorDriver.getContext(), UserVectorDriver.indexedDataset)


      Writer.writeDRM_itemItem(item(0),"src/test/resources/ItemSimilarityMatrix")

      UserVectorDriver.stop()



  }
}
