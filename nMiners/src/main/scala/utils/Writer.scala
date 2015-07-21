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

  def writeDRM(drm:CheckpointedDrm[Int],path:String,schema:Schema,indexedDataset: IndexedDataset)(implicit sc: DistributedContext):Unit = {
    val matrixWithNames = indexedDataset.create(drm,indexedDataset.columnIDs,indexedDataset.columnIDs)
    matrixWithNames.dfsWrite(path,schema)(sc)
  }

  def writeDRM(drm:CheckpointedDrm[Int],path:String):Unit = {
    val matrixWithNames = indexedDataset.create(drm,indexedDataset.rowIDs,indexedDataset.columnIDs)
    matrixWithNames.dfsWrite(path,writeSchema)(context)
  }
}
