package com.example

import com.google.common.collect.{BiMap, HashBiMap}
import org.apache.mahout.math.cf.SimilarityAnalysis
import org.apache.mahout.math.indexeddataset.{IndexedDatasetWriteBooleanSchema, IndexedDataset, DefaultIndexedDatasetElementReadSchema}
import org.apache.mahout.sparkbindings.{SparkEngine, mahoutSparkContext}

object CooccurrenceDriver extends App {
  implicit val mc = mahoutSparkContext(masterUrl = "local", appName = "CooccurrenceDriver")

  val actions = readActions(toAction(args.toList).toArray)

  // actions.map creates an array of just the IndeedDatasets
  val indicatorMatrices = SimilarityAnalysis.cooccurrencesIDSs(
    actions.map(a => a._2))

  // zip a pair of arrays into an array of pairs, reattaching the action names
  val indicatorDescriptions = actions.map(a => a._1).zip(indicatorMatrices)
  writeIndicators(indicatorDescriptions)

  /**
   * Write indicatorMatrices to the output dir in the default format
   * for indexing by a search engine.
   */
  def writeIndicators( indicators: Array[(String, IndexedDataset)]) = {
    for (indicator <- indicators ) {
      // create a name based on the type of indicator
      val indicatorDir = indicator._1
      indicator._2.dfsWrite(
        indicatorDir,
        // Schema tells the writer to omit LLR strengths
        // and format for search engine indexing
        IndexedDatasetWriteBooleanSchema)
    }
  }

  def toAction(args: List[String]): List[(String, String)] = args match {
    case Nil => Nil
    case h1 :: h2 :: tail => (h1, h2) :: toAction(tail)
  }

  /**
   * Read files of element tuples and create IndexedDatasets one per action. These
   * share a userID BiMap but have their own itemID BiMaps
   */
  def readActions(actionInput: Array[(String, String)]): Array[(String, IndexedDataset)] = {
    var actions = Array[(String, IndexedDataset)]()

    val userDictionary: BiMap[String, Int] = HashBiMap.create()

    // The first action named in the sequence is the "primary" action and
    // begins to fill up the user dictionary
    for ( actionDescription <- actionInput ) {// grab the path to actions
    val action: IndexedDataset = SparkEngine.indexedDatasetDFSReadElements(
        actionDescription._2,
        schema = DefaultIndexedDatasetElementReadSchema,
        existingRowIDs = userDictionary)
      userDictionary.putAll(action.rowIDs)
      // put the name in the tuple with the indexedDataset
      actions = actions :+ (actionDescription._1, action)
    }

    // After all actions are read in the userDictonary will contain every user seen,
    // even if they may not have taken all actions . Now we adjust the row rank of
    // all IndxedDataset's to have this number of rows
    // Note: this is very important or the cooccurrence calc may fail
    val numUsers = userDictionary.size() // one more than the cardinality

    val resizedNameActionPairs = actions.map { a =>
      //resize the matrix by, in effect by adding empty rows
      val resizedMatrix = a._2.create(a._2.matrix, userDictionary, a._2.columnIDs).newRowCardinality(numUsers)
      (a._1, resizedMatrix) // return the Tuple of (name, IndexedDataset)
    }
    resizedNameActionPairs // return the array of Tuples
  }
}
