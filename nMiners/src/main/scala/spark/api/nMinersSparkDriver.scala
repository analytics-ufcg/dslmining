package spark.api

import org.apache.mahout.drivers
import org.apache.mahout.drivers.{MahoutSparkOptionParser}
import org.apache.mahout.math.drm.{CheckpointedDrm, DistributedContext}
import org.apache.mahout.math.indexeddataset.{Schema, IndexedDataset}
import scala.collection.{mutable, TraversableOnce}
import scala.collection.immutable.HashMap
import org.apache.mahout.sparkbindings._

/**
 * Created by andryw on 14/07/15.
 */
abstract class nMinersSparkDriver extends drivers.MahoutSparkDriver{

  implicit var sparkMasterUrl : String = "blablabla"

  private final val ItemSimilarityOptions = HashMap[String, Any](
    "maxPrefs" -> 500,
    "maxSimilaritiesPerItem" -> 100,
    "appName" -> "ItemSimilarityDriver")

  def createParse: Unit = {
    var sparkUrl: String = sparkMasterUrl
    parser = new MahoutSparkOptionParser(programName = "spark-itemsimilarity") {
      head("spark-itemsimilarity", "Mahout 0.11.0")

      //Input output options, non-driver specific
      parseIOOptions(numInputs = 2)

      //Algorithm control options--driver specific
      opts = opts ++ ItemSimilarityOptions
      note("\nAlgorithm control options:")
      opt[Int]("maxPrefs") abbr "mppu" action { (x, options) =>
        options + ("maxPrefs" -> x)
      } text ("Max number of preferences to consider per user (optional). Default: " +
        ItemSimilarityOptions("maxPrefs")) validate { x =>
        if (x > 0) success else failure("Option --maxPrefs must be > 0")
      }

      // not implemented in SimilarityAnalysis.cooccurrence
      // threshold, and minPrefs
      // todo: replacing the threshold with some % of the best values and/or a
      // confidence measure expressed in standard deviations would be nice.

      opt[Int]('m', "maxSimilaritiesPerItem") action { (x, options) =>
        options + ("maxSimilaritiesPerItem" -> x)
      } text ("Limit the number of similarities per item to this number (optional). Default: " +
        ItemSimilarityOptions("maxSimilaritiesPerItem")) validate { x =>
        if (x > 0) success else failure("Option --maxSimilaritiesPerItem must be > 0")
      }

      //Driver notes--driver specific
      note("\nNote: Only the Log Likelihood Ratio (LLR) is supported as a similarity measure.")

      //Input text format
      parseElementInputSchemaOptions()

      //How to search for input
      parseFileDiscoveryOptions()

      //Drm output schema--not driver specific, drm specific
      parseIndexedDatasetFormatOptions()

      //Spark config options--not driver specific
      parseSparkOptions() (sparkConf)
      opts = opts-("master")
      opts = opts+("master"-> sparkUrl)

      //Jar inclusion, this option can be set when executing the driver from compiled code, not when from CLI
      parseGenericOptions()

      help("help") abbr ("h") text ("prints this usage text\n")

    }
  }

  override def start(): Unit = {
    createParse
    if (!_useExistingContext) {
      sparkConf.set("spark.kryo.referenceTracking", "false")
        .set("spark.kryoserializer.buffer.mb", "200")// this is default for Mahout optimizer, change it with -D option

      if (parser.opts("sparkExecutorMem").asInstanceOf[String] != "")
        sparkConf.set("spark.executor.memory", parser.opts("sparkExecutorMem").asInstanceOf[String])
      //else leave as set in Spark config
     // val trav = TraversableOnce[String]
      val customJar = new mutable.ArrayBuffer[String]()
      customJar.append(sparkConf.get("spark.jars"))

      mc = mahoutSparkContext(
        masterUrl = parser.opts("master").asInstanceOf[String],
        appName = parser.opts("appName").asInstanceOf[String],
        customJars = customJar,
        sparkConf = sparkConf)
    }
  }

  override def stop(): Unit = {
    super.stop()
  }

}
