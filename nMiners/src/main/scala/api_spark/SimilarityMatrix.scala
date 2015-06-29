package api_spark

import org.apache.mahout.drivers.ItemSimilarityDriver


object SimilarityMatrix extends App {
  val InFile = "data/actions.csv"
  val OutPath = "data/similarity-matrices/"

  def run(inputFile: String, outPath: String, masterNode:String, filter:String) ={
    ItemSimilarityDriver.main(Array(
      "--input", inputFile,
      "--output", outPath,
      "--master", masterNode,
      "--filter1",filter,
      "--inDelim", ",",
      "--itemIDColumn", "2",
      "--rowIDColumn", "0",
      "--filterColumn", "1",
      "--writeAllDatasets"))
  }

  run(InFile,OutPath, "local",  "purchase")
}