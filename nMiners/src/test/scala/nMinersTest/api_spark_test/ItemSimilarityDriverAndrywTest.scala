package nMinersTest.api_spark_test

import api_spark.ItemSimilarityDriverAndryw
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkConf
import org.scalatest.{Matchers, FlatSpec}


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.mahout.math.indexeddataset.{BiDictionary, IndexedDataset}
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.scalatest.{ConfigMap, FunSuite}
import org.apache.mahout.sparkbindings._
import org.apache.mahout.math.drm._
import org.apache.mahout.math.scalabindings._

import scala.collection.immutable.HashMap

//todo: take out, only for temp tests

import org.apache.mahout.math.scalabindings._
import RLikeOps._
import RLikeDrmOps._

/**
 * Created by andryw on 02/07/15.
 */
class ItemSimilarityDriverAndrywTest extends FlatSpec with Matchers{
  def tokenize(a: Iterable[String]): Iterable[String] = {
    var r: Iterable[String] = Iterable()
    a.foreach { l =>
      l.split("\t").foreach { s =>
        r = r ++ s.split("[\t ]")
      }
    }
    r
  }

    "Level one" should "execute first mapreduce" in {


      val InDir = "in-dir/"
      val InFilename = "in-file.tsv"
      val InPath = InDir + InFilename

      val OutPath = "similarity-matrices"

      val lines = Array(
        "0,0,1",
        "0,1,1",
        "1,2,1",
        "1,3,1",
        "2,4,1",
        "3,0,1",
        "3,3,1")

      val Answer = tokenize(Iterable(
        "0\t1:1.7260924347106847",
        "3\t2:1.7260924347106847",
        "1\t0:1.7260924347106847",
        "4",
        "2\t3:1.7260924347106847"))

      var masterUrl = "local[3]"
      var mahoutCtx = mahoutSparkContext(masterUrl = masterUrl,
        appName = "MahoutLocalContext",
        // Do not run MAHOUT_HOME jars in unit tests.
        addMahoutJars = false,
        sparkConf = new SparkConf()
          .set("spark.kryoserializer.buffer.mb", "15")
          .set("spark.akka.frameSize", "30")
          .set("spark.default.parallelism", "10")

      )

      // this creates one part-0000 file in the directory
      mahoutCtx.parallelize(lines).coalesce(1, shuffle = true).saveAsTextFile(InDir)

      // to change from using part files to a single .tsv file we'll need to use HDFS
      val fs = FileSystem.get(new Configuration())
      //rename part-00000 to something.tsv
      fs.rename(new Path(InDir + "part-00000"), new Path(InPath))

      ItemSimilarityDriverAndryw.useContext(mahoutCtx)

      // local multi-threaded Spark with default HDFS
      val (a,b) = ItemSimilarityDriverAndryw.maina(Array(
        "--input", InPath,
        "--output", OutPath,
        "--master", masterUrl))


//      val similarityLines = mahoutCtx.textFile(OutPath + "/similarity-matrix/").collect.toIterable
//      tokenize(similarityLines) should contain theSameElementsAs Answer
    }
}
