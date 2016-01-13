package utils

import org.apache.log4j.Logger
import org.apache.mahout.math.drm.{CheckpointedDrm, DistributedContext}
import org.apache.mahout.math.indexeddataset.{BiDictionary, IndexedDataset, Schema}
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import spark.api.UserVectorDriver
;

/**
 * Created by andryw on 20/07/15.
 */
object Writer {

  var writeSchema: Schema = _
  var indexedDataset: IndexedDataset = _
  var context: DistributedContext = _

  /**
    * it saves the matrix values that are represented by DRM along the user names and item names.
    * The drm does not have the user and item names. These were replaced by indexes in order to improve the calculation.
    *This way, the indexedDataset is used to make a mapping the items to user names and item .
   * @param drm
   *            DRM object used for multiplication is a user-item matrix
   * @param path
   *             Outputh path
   */

  def writeDRM_itemItem(drm:CheckpointedDrm[Int],path:String):Unit = {
    val matrixWithNames = indexedDataset.create(drm,indexedDataset.columnIDs,indexedDataset.columnIDs)
    matrixWithNames.dfsWrite(path,writeSchema)(context)
  }

  /**
   * it saves the matrix values that are represented by DRM along the user names and item names.
   * The drm does not have the user and item names. These were replaced by indexes in order to improve the calculation.
   *This way, the indexedDataset is used to make a mapping the items to user names and item.
   * @param drm
   *            DRM object used for multiplication is an item-item matrix
   * @param path
   *             Outputh path
   */

  def writeDRM_userItem(drm:CheckpointedDrm[Int],path:String):Unit = {
    print("Vai escrever")
    //val matrixWithNames = indexedDataset.create(drm,indexedDataset.rowIDs,indexedDataset.columnIDs)
    //matrixWithNames.dfsWrite(path,writeSchema)(context)

    val indexedDataSetSpark = new IndexedDatasetSpark(drm.uncache(), indexedDataset.rowIDs, indexedDataset.columnIDs)
    //indexedDataset.matrix.checkpoint().dfsWrite(path)
    //indexedDataSetSpark.dfsWrite(path, writeSchema)(UserVectorDriver.getContext())
    val myWriter = new TextDelimitedIndexedDatasetWriter(writeSchema, sort = false)(UserVectorDriver.getContext())

    myWriter.writeTo(indexedDataSetSpark, path)
    print("Escreveu!")
  }


}

class TextDelimitedIndexedDatasetWriter(val writeSchema: Schema, val sort: Boolean = true)
  (implicit val mc: DistributedContext)
  extends MyWriter
