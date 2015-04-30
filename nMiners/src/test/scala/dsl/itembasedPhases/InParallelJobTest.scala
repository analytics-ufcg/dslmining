package dsl.itembasedPhases

/**
 * Created by tales on 30/04/15.
 */

import collection.mutable.Stack
import org.scalatest._

class InParallelJobTest extends FlatSpec with Matchers {

  "The Job output results" should "be the same if executed in parallel or sequential" in {

    val outputParallelPath1 = "src/main/resources/wordCount1/part-r-00000"
    val outputParallelPath2 = "src/main/resources/wordCount2/part-r-00000"
    val outputSequentialPath = "src/main/resources/wordCount3/part-r-00000"

    var outputs: List[String] = List()

    val text1 = loadTextFromFile(outputParallelPath1)
    val text2 = loadTextFromFile(outputParallelPath2)
    val text3 = loadTextFromFile(outputSequentialPath)

    outputs = text1 :: outputs
    outputs = text2 :: outputs
    outputs = text3 :: outputs

    //It must be at least 2 elements to check its equality
    var index = 0
    for ( index <- 0 to outputs.size - 2 ){
      outputs.apply(index) should be (outputs.apply(index + 1))
    }




  }


  def loadTextFromFile (filePath : String)  = {
    val source = scala.io.Source.fromFile(filePath)
    val lines = try source.mkString finally source.close()
    lines
  }


}