package api_spark
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


import org.apache.mahout.drivers.{MahoutOptionParser, MahoutSparkDriver}
import org.apache.mahout.math.drm.DrmLike
import org.apache.mahout.math.indexeddataset.Schema

import scala.collection.immutable.HashMap

/**
 * Command line interface for org.apache.mahout.math.cf.SimilarityAnalysis#cooccurrencesIDSs. Reads text lines
 * that contain (row id, column id, ...). The IDs are user specified strings which will be preserved in the output.
 * The individual elements will be accumulated into a matrix like
 * org.apache.mahout.math.indexeddataset.IndexedDataset and
 * org.apache.mahout.math.cf.SimilarityAnalysis#cooccurrencesIDSs]] will be used to calculate row-wise
 * self-similarity, or when using filters or two inputs, will generate two matrices and calculate both the
 * self-similarity of the primary matrix and the row-wise similarity of the primary to the secondary. Returns one
 * or two directories of text files formatted as specified in the options. The options allow flexible control of the
 * input schema, file discovery, output schema, and control of algorithm parameters. To get help run
 * {{{mahout spark-itemsimilarity}}} for a full explanation of options. To process simple elements of text delimited
 * values (userID,itemID) with or without a strengths and with a separator of tab, comma, or space, you can specify
 * only the input and output file and directory--all else will default to the correct values. Each output line will
 * contain the Item ID and similar items sorted by LLR strength descending.
 * @note To use with a Spark cluster see the --master option, if you run out of heap space check
 *       the --sparkExecutorMemory option. Other org.apache.spark.SparkConf key value pairs can be with the -D:k=v
 *       option.
 */
object ItemSimilarityDriver extends MahoutSparkDriver {
  // define only the options specific to ItemSimilarity
  private final val ItemSimilarityOptions = HashMap[String, Any](
    "maxPrefs" -> 500,
    "maxSimilaritiesPerItem" -> 100,
    "appName" -> "ItemSimilarityDriver")

  private var writeSchema: Schema = _
  private var readSchema1: Schema = _
  private var readSchema2: Schema = _
  var userVectorDrm: Array[DrmLike[Int]] = _
  var idssItemSimilarity:List[DrmLike[Int]]=_
  /**
   * Entry point, not using Scala App trait
   * @param args  Command line args, if empty a help message is printed.
   */
  override def main(args: Array[String]): Unit = {
    parser.parse(args, parser.opts) map { opts =>
      parser.opts = opts
      process()
    }
  }

  override protected def start() : Unit = {

    super.start()

    readSchema1 = new Schema("delim" -> parser.opts("inDelim").asInstanceOf[String],
      "filter" -> parser.opts("filter1").asInstanceOf[String],
      "rowIDColumn" -> parser.opts("rowIDColumn").asInstanceOf[Int],
      "columnIDPosition" -> parser.opts("itemIDColumn").asInstanceOf[Int],
      "filterColumn" -> parser.opts("filterColumn").asInstanceOf[Int])

    if ((parser.opts("filterColumn").asInstanceOf[Int] != -1 && parser.opts("filter2").asInstanceOf[String] != null)
      || (parser.opts("input2").asInstanceOf[String] != null && !parser.opts("input2").asInstanceOf[String].isEmpty )){
      // only need to change the filter used compared to readSchema1
      readSchema2 = new Schema(readSchema1) += ("filter" -> parser.opts("filter2").asInstanceOf[String])

    }

    writeSchema = new Schema(
      "rowKeyDelim" -> parser.opts("rowKeyDelim").asInstanceOf[String],
      "columnIdStrengthDelim" -> parser.opts("columnIdStrengthDelim").asInstanceOf[String],
      "omitScore" -> parser.opts("omitStrength").asInstanceOf[Boolean],
      "elementDelim" -> parser.opts("elementDelim").asInstanceOf[String])
  }

  override def process(): Unit = {
//    start()

    val idss = SimilarityAnalysis.cooccurrencesIDSs(userVectorDrm, parser.opts("randomSeed").asInstanceOf[Int],
      parser.opts("maxSimilaritiesPerItem").asInstanceOf[Int], parser.opts("maxPrefs").asInstanceOf[Int])
    idssItemSimilarity = idss
//    // todo: allow more than one cross-similarity matrix?
//    idss(0).dfsWrite(parser.opts("output").asInstanceOf[String] + "similarity-matrix")
//    if(idss.length > 1)
//      idss(1).dfsWrite(parser.opts("output").asInstanceOf[String] + "cross-similarity-matrix")

//    stop()
  }

  def run(userVector: Array[DrmLike[Int]], parserA: MahoutOptionParser, args: Array[String] ): List[DrmLike[Int]]= {
        userVectorDrm = userVector
        print(parser == null)
        parser = parserA
        main(args)
        idssItemSimilarity
  }

  def run(userVector: Array[DrmLike[Int]], args: Array[String] ) = {
        userVectorDrm = userVector
        main(args)
  }

}
