package utils

import org.apache.mahout.math.drm.{CheckpointedDrm, DistributedContext}
import org.apache.mahout.math.indexeddataset.{IndexedDataset, Schema}

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
    val matrixWithNames = indexedDataset.create(drm,indexedDataset.rowIDs,indexedDataset.columnIDs)
    matrixWithNames.dfsWrite(path,writeSchema)(context)
  }
}
