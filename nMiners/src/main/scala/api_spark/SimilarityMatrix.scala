package api_spark

import org.apache.mahout.drivers.ItemSimilarityDriver

/**
 * The code below produces a similarity matrix between all the itens. If a similarity between two itens is
 * equal to zero, then the code will not put a relation between those itens inside the matrix.
 */


object SimilarityMatrix extends App {
  val InFile = "data/actions.csv" //Input Data
  val OutPath = Some("data/similarity-matrices/") // Output path where the matrix should be after the execution

  //The method below takes the correct parameters in order to call the Main from ItemSimilarity object
  def run(inputFile: String, outPath: Option[String], masterNode:String) ={
    ItemSimilarityDriver.main(Array(
      "--input", inputFile,
      "--output", outPath.getOrElse(""),
      "--master", masterNode
    ))
  }
  run(InFile,OutPath, "local")
}