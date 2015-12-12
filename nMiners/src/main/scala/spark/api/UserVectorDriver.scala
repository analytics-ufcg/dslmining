/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package spark.api

import org.apache.mahout.common.HDFSPathSearch
import org.apache.mahout.drivers.MahoutOptionParser
import org.apache.mahout.math.DenseVector
import org.apache.mahout.math.drm.{DistributedContext, DrmLike}
import org.apache.mahout.math.indexeddataset.{IndexedDataset, Schema, indexedDatasetDFSReadElements}
import org.apache.mahout.sparkbindings.drm.CheckpointedDrmSparkOps
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.mahout.sparkbindings.drm
import utils.Holder

/**
 * **************************************************************************
 * This code was adaptation of org.apache.mahout.drivers.ItemSimilarityDriver
 * *************************************************************************
 *  
 * Reads text lines that contain (row id, column id, ...). The IDs are user specified strings which will be preserved in the output.
 * The individual elements will be accumulated into a matrix like
 * org.apache.mahout.math.indexeddataset.IndexedDataset will be used to calculate row-wise
 * self-similarity. To process simple elements of text delimited
 * values (userID,itemID) with or without a strengths and with a separator of tab, comma, or space, you can specify
 * only the input and output file and directory--all else will default to the correct values. Each output line will
 * contain the Item ID and similar items sorted by LLR strength descending.
 * @note To use with a Spark cluster see the --master option, if you run out of heap space check
 *       the --sparkExecutorMemory option. Other org.apache.spark.SparkConf key value pairs can be with the -D:k=v
 *       option.
 */
object UserVectorDriver extends nMinersSparkDriver{
  //The object parser needs to be visible outside. But parser is protected.
  def getParser(): MahoutOptionParser = parser
  def getContext(): DistributedContext = mc

  var writeSchema: Schema = _
  private var readSchema1: Schema = _
  private var readSchema2: Schema = _
  var drmsUserVector:Array[DrmLike[Int]] = _
  var indexedDataset: IndexedDataset = _

  /**
   * Entry point, not using Scala App trait
   * It's necessary start the spark before run the main. Use start()
   * @param args  Command line args, if empty a help message is printed.
   */
  override def main(args: Array[String]): Unit = {

    require(mc != null,"mc is null. Did you start spark?")
    require(sparkConf != null,"sparkConf is null. Did you start spark?")
    require(parser != null,"parser is null. Did you start spark?")
//    assert(mc.asInstanceOf[SparkDistributedContext].sc.stop(),"context is stoped. Did you start spark?")

    parser.parse(args, parser.opts) map { opts =>
      parser.opts = opts
      process()
    }
  }

 def start(master:String, jar:String): Unit ={
   Class.forName("org.apache.mahout.math.DenseVector")
   Class.forName("org.apache.mahout.math.Vector")
   println("forname ok")
   sparkMasterUrl = master
   sparkConf.setJars(Array(jar))
   sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
   sparkConf.setExecutorEnv("SPARK_EXECUTOR_MEMORY", "1G")
   //sparkConf.s
   mc
   createParse
   super.start()
 }

  def createSchemas: Unit = {
    readSchema1 = new Schema("delim" -> parser.opts("inDelim").asInstanceOf[String],
      "filter" -> parser.opts("filter1").asInstanceOf[String],
      "rowIDColumn" -> parser.opts("rowIDColumn").asInstanceOf[Int],
      "columnIDPosition" -> parser.opts("itemIDColumn").asInstanceOf[Int],
      "filterColumn" -> parser.opts("filterColumn").asInstanceOf[Int])

    if ((parser.opts("filterColumn").asInstanceOf[Int] != -1 && parser.opts("filter2").asInstanceOf[String] != null)
      || (parser.opts("input2").asInstanceOf[String] != null && !parser.opts("input2").asInstanceOf[String].isEmpty)) {
      // only need to change the filter used compared to readSchema1
      readSchema2 = new Schema(readSchema1) += ("filter" -> parser.opts("filter2").asInstanceOf[String])

    }

    writeSchema = new Schema(
      "rowKeyDelim" -> parser.opts("rowKeyDelim").asInstanceOf[String],
      "columnIdStrengthDelim" -> parser.opts("columnIdStrengthDelim").asInstanceOf[String],
      "omitScore" -> parser.opts("omitStrength").asInstanceOf[Boolean],
      "elementDelim" -> parser.opts("elementDelim").asInstanceOf[String])
  }

  def readIndexedDatasets: Array[IndexedDataset] = {

    val inFiles = HDFSPathSearch(parser.opts("input").asInstanceOf[String],
      parser.opts("filenamePattern").asInstanceOf[String], parser.opts("recursive").asInstanceOf[Boolean]).uris
    val inFiles2 = if (parser.opts("input2") == null || parser.opts("input2").asInstanceOf[String].isEmpty) ""
    else HDFSPathSearch(parser.opts("input2").asInstanceOf[String], parser.opts("filenamePattern").asInstanceOf[String],
      parser.opts("recursive").asInstanceOf[Boolean]).uris

    if (inFiles.isEmpty) {
      Array()
    } else {

      val datasetA = indexedDatasetDFSReadElements(inFiles, readSchema1)
      if (parser.opts("writeAllDatasets").asInstanceOf[Boolean])
        datasetA.dfsWrite(parser.opts("output").asInstanceOf[String] + "../input-datasets/primary-interactions",
          schema = writeSchema)

      // The case of reading B can be a bit tricky when the exact same row IDs don't exist for A and B
      // Here we assume there is one row ID space for all interactions. To do this we calculate the
      // row cardinality only after reading in A and B (or potentially C...) We then adjust the cardinality
      // so all match, which is required for the math to work.
      // Note: this may leave blank rows with no representation in any DRM. Blank rows need to
      // be supported (and are at least on Spark) or the row cardinality adjustment will not work.
      val datasetB = if (!inFiles2.isEmpty) {
        // get cross-cooccurrence interactions from separate files
        val datasetB = indexedDatasetDFSReadElements(inFiles2, readSchema2, existingRowIDs = Some(datasetA.rowIDs))

        datasetB

      } else if (parser.opts("filterColumn").asInstanceOf[Int] != -1
        && parser.opts("filter2").asInstanceOf[String] != null) {

        // get cross-cooccurrences interactions by using two filters on a single set of files
        val datasetB = indexedDatasetDFSReadElements(inFiles, readSchema2, existingRowIDs = Some(datasetA.rowIDs))

        datasetB

      } else {
        null.asInstanceOf[IndexedDatasetSpark]
      }
      if (datasetB != null.asInstanceOf[IndexedDataset]) {
        // do AtB calc
        // true row cardinality is the size of the row id index, which was calculated from all rows of A and B
        val rowCardinality = datasetB.rowIDs.size // the authoritative row cardinality

        val returnedA = if (rowCardinality != datasetA.matrix.nrow) datasetA.newRowCardinality(rowCardinality)
        else datasetA // this guarantees matching cardinality

        val returnedB = if (rowCardinality != datasetB.matrix.nrow) datasetB.newRowCardinality(rowCardinality)
        else datasetB // this guarantees matching cardinality

        if (parser.opts("writeAllDatasets").asInstanceOf[Boolean])
          datasetB.dfsWrite(parser.opts("output").asInstanceOf[String] + "../input-datasets/secondary-interactions",
            schema = writeSchema)
        Array(returnedA, returnedB)
      } else Array(datasetA)
    }
  }

  override def process(): Unit = {
    createSchemas
    mc
    for(a <- 1 to 100) println("uoui")
    val indexedDatasets = readIndexedDatasets
    indexedDataset = indexedDatasets(0)
    val randomSeed = parser.opts("randomSeed").asInstanceOf[Int]
    val maxInterestingItemsPerThing = parser.opts("maxSimilaritiesPerItem").asInstanceOf[Int]
    val maxNumInteractions = parser.opts("maxPrefs").asInstanceOf[Int]
    val drms = indexedDatasets.map(_.matrix.asInstanceOf[DrmLike[Int]])
    drmsUserVector = drms
    val a = indexedDatasets(0).create(drms(0), indexedDatasets(0).columnIDs, indexedDatasets(0).columnIDs)
    val   writeSchema = new Schema(
      "rowKeyDelim" -> parser.opts("rowKeyDelim").asInstanceOf[String],
      "columnIdStrengthDelim" -> parser.opts("columnIdStrengthDelim").asInstanceOf[String],
      "omitScore" -> parser.opts("omitStrength").asInstanceOf[Boolean],
      "elementDelim" -> parser.opts("elementDelim").asInstanceOf[String])
  }

  /**
   * The method below executes main method and returns the DRMS
   * @param args Command line args, if empty a help message is printed. The mainly
   *             args are:  --input", the inputFile
                            --output the outputpaht
                            --master the address of the cluster or "local"
   * @return a drms who representes the UserVector
   */
  def run(args: Array[String]):Array[DrmLike[Int]] ={
    main(args)
    drmsUserVector
  }

//  def writeDRM(path:String):Unit = {
//    super.writeDRM(this.drmsUserVector(0),path,this.writeSchema,this.indexedDataset)(mc)
//  }
}