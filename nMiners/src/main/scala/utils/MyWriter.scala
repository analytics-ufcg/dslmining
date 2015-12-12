package utils

import org.apache.log4j.Logger
import org.apache.mahout.math.drm.DistributedContext
import org.apache.mahout.math.indexeddataset._
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.spark.SparkContext._
import org.apache.mahout.math.RandomAccessSparseVector
import org.apache.mahout.math.drm.{DrmLike, DrmLikeOps, DistributedContext, CheckpointedDrm}
import org.apache.mahout.sparkbindings._
import scala.collection.JavaConversions._

/**
  * Created by lucas on 10/12/15.
  */

object Holder extends Serializable{
  @transient lazy val logger = Logger.getLogger(this.getClass.getCanonicalName)
}
trait MyWriter extends Writer[IndexedDatasetSpark]{

  protected def writer(
                        mc: DistributedContext,
                        writeSchema: Schema,
                        dest: String,
                        indexedDataset: IndexedDatasetSpark,
                        sort: Boolean = true): Unit = {
    @transient lazy val logger = Logger.getLogger(this.getClass.getCanonicalName)
    try {
      val rowKeyDelim = writeSchema("rowKeyDelim").asInstanceOf[String]
      val columnIdStrengthDelim = writeSchema("columnIdStrengthDelim").asInstanceOf[String]
      val elementDelim = writeSchema("elementDelim").asInstanceOf[String]
      val omitScore = writeSchema("omitScore").asInstanceOf[Boolean]
      //instance vars must be put into locally scoped vals when put into closures that are
      //executed but Spark

      require (indexedDataset != null ,"No IndexedDataset to write")
      require (!dest.isEmpty,"No destination to write to")

      val matrix = indexedDataset.matrix.checkpoint()
      val rowIDDictionary = indexedDataset.rowIDs
      val rowIDDictionary_bcast = mc.broadcast(rowIDDictionary)

      val columnIDDictionary = indexedDataset.columnIDs
      val columnIDDictionary_bcast = mc.broadcast(columnIDDictionary)

      matrix.rdd.map { case (rowID, itemVector) =>

        // turn non-zeros into list for sorting
        var itemList = List[(Int, Double)]()
        for (ve <- itemVector.nonZeroes) {
          itemList = itemList :+ (ve.index, ve.get)
        }
        //sort by highest value descending(-)
        val vector = if (sort) itemList.sortBy { elem => -elem._2 } else itemList

        // first get the external rowID token
        if (!vector.isEmpty){

          Holder.logger.info("I got this rowID "+rowID)
          var line = rowIDDictionary_bcast.value.inverse.getOrElse(rowID, "INVALID_ROW_ID") + rowKeyDelim
          // for the rest of the row, construct the vector contents of elements (external column ID, strength value)
          for (item <- vector) {
            line += columnIDDictionary_bcast.value.inverse.getOrElse(item._1, "INVALID_COLUMN_ID")
            if (!omitScore) line += columnIdStrengthDelim + item._2
            line += elementDelim
          }
          // drop the last delimiter, not needed to end the line
          line.dropRight(1)
        } else {//no items so write a line with id but no values, no delimiters
          rowIDDictionary_bcast.value.inverse.getOrElse(rowID, "INVALID_ROW_ID")
        } // "if" returns a line of text so this must be last in the block
      }
        .saveAsTextFile(dest)

    }catch{
      case cce: ClassCastException => {
        logger.error("Schema has illegal values"); throw cce}
    }
  }
}
