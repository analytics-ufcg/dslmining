package api_spark

import org.apache.mahout.drivers.ItemSimilarityDriver


object SimilarityMatrix extends App {
  val InFile = "data/actions.csv"
  val OutPath = "data/similarity-matrices/"

  def run(inputFile: String, outPath: Option[String], masterNode:String) ={
    ItemSimilarityDriver.main(Array(
      "--input", inputFile,
      "--output", outPath.getOrElse(""),
      "--master", masterNode
    ))
  }

//  run(InFile,OutPath, "local")
}