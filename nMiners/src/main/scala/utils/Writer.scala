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
   * The method below takes a DRM object and a path in order to create a file that represents the multiplication among indexedDataset's columns
   * based on DRM object.
   * @param drm
   *            DRM object used for multiplication
   * @param path
   *             Outputh path
   */

  def writeDRM_itemItem(drm:CheckpointedDrm[Int],path:String):Unit = {
    val matrixWithNames = indexedDataset.create(drm,indexedDataset.columnIDs,indexedDataset.columnIDs)
    matrixWithNames.dfsWrite(path,writeSchema)(context)
  }

  /**
   * The method below takes a DRM object and a path in order to create a file that represents the multiplication among indexedDataset's rows and columns
   * based on DRM object.
   * @param drm
   *            DRM object used for multiplication
   * @param path
   *             Outputh path
   */

  def writeDRM_userItem(drm:CheckpointedDrm[Int],path:String):Unit = {
    val matrixWithNames = indexedDataset.create(drm,indexedDataset.rowIDs,indexedDataset.columnIDs)
    matrixWithNames.dfsWrite(path,writeSchema)(context)
  }
}
