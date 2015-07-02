package driver

import org.apache.mahout.drivers.ItemSimilarityDriver


object Driver extends App {
    val InFile = "data/actions.csv"
    val OutPath = "data/similarity-matrices/"


    ItemSimilarityDriver.main(Array(
      "--input", InFile,
      "--output", OutPath,
      "--master", "local",
      "--filter1", "purchase",
      "--inDelim", ",",
      "--itemIDColumn", "2",
      "--rowIDColumn", "0",
      "--filterColumn", "1",
      "--writeAllDatasets"))
}