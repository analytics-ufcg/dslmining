/**
 * Created by tales on 30/04/15.
 */
object TryScala extends App{

  //val source = scala.io.Source.fromFile("src/main/resources/wordCount1/part-r-00000")
  //val lines = try source.mkString finally source.close()

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

  var index = 0
  for ( index <- 0 to (outputs.size - 1) ){
    print (outputs.apply(index))
    print ("\n")
  }


  def loadTextFromFile (filePath : String)  = {
    val source = scala.io.Source.fromFile(filePath)
    val lines = try source.mkString finally source.close()
    lines.toString
  }

}
