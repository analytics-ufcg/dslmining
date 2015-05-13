package api

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.mahout.cf.taste.hadoop.item.{VectorAndPrefsWritable, VectorOrPrefWritable}
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math._
import utils.Implicits._
import utils.MapReduceUtils

/**
 * This class represents the first step towards performing matrix multiplication between the co-occurrence matrix and
 * the item-user vectors: it reads the preference values matrix from file.
 */
class CooccurrenceColumnWrapperMapper extends Mapper[VarIntWritable,VectorWritable,  VarIntWritable,VectorOrPrefWritable] {
  val vectorOrPref: VectorOrPrefWritable = new VectorOrPrefWritable

  override def  map(key: VarIntWritable, value: VectorWritable , context:Mapper[VarIntWritable,VectorWritable,  VarIntWritable,VectorOrPrefWritable]#Context) = {

    val similarityMatrixRow:Vector = value.get()
    similarityMatrixRow.set(key.get(), Double.NaN)

    context.write(key, new VectorOrPrefWritable(similarityMatrixRow))
  }

}

/**
 * This step is necessary to fit the need of having an item-user vector instead of user-item vector.
 */
class UserVectorSplitterMapper extends Mapper[VarLongWritable,VectorWritable,  VarIntWritable,VectorOrPrefWritable] {
  override def map(key: VarLongWritable ,
                   value:VectorWritable,
                   context:Mapper[VarLongWritable,VectorWritable,
                   VarIntWritable,VectorOrPrefWritable]#Context)= {
    val userID:Long = key get
    val userVector:Vector = value get
    val it = userVector.nonZeroes() iterator
    val itemIndexWritable:VarIntWritable = new VarIntWritable()
    it.foreach((item: Element) => {
      val itemIndex = item.index()
      val preferenceValue:Double = item get;
      itemIndexWritable.set(itemIndex)
      context write(itemIndexWritable,  new VectorOrPrefWritable(userID,preferenceValue toFloat))
    })
  }
}

/**
 * Combines the output from the previous two steps, then starts the multiplication of the item-item co-occurrence matrix with the item-user vectors.
 */
class ToVectorAndPrefReducer extends Reducer[VarIntWritable, VectorOrPrefWritable, VarIntWritable, VectorAndPrefsWritable]{
  val vectorAndPrefs: VectorAndPrefsWritable = new VectorAndPrefsWritable

  override def reduce( key: VarIntWritable,
                        values: java.lang.Iterable[VectorOrPrefWritable],
                        context: Reducer[VarIntWritable, VectorOrPrefWritable,
                        VarIntWritable, VectorAndPrefsWritable]#Context) = {

    val userIDs: java.util.List[java.lang.Long]= new java.util.ArrayList()
    val prefValues: java.util.List[java.lang.Float] =  new java.util.ArrayList()
    var similarityMatrixColumn: Vector = null


    values.foreach((vector: VectorOrPrefWritable) => {
      if (vector.getVector == null) {
        userIDs.add(vector.getUserID)
        prefValues.add(vector.getValue)
      }
      else {
        if (similarityMatrixColumn != null) {
          throw new IllegalStateException("Found two similarity-matrix columns for item index " + key.get)
        }
        similarityMatrixColumn = vector.getVector
      }
    })

    if (similarityMatrixColumn != null) {

      vectorAndPrefs.set(similarityMatrixColumn, userIDs,prefValues)
      context.write(key, vectorAndPrefs)
    }

  }
}

/**
 *
 */
object PrepareMatrixGenerator {
      def runJob(inputPath1:String,inputPath2:String,outPutPath:String, inputFormatClass:Class[_<:FileInputFormat[_,_]],
                 outputFormatClass:Class[_<:FileOutputFormat[_,_]], deleteFolder : Boolean,
                 numReduceTasks : Option[Int] = None): Unit ={
        MapReduceUtils.run2MappersJob("Prepare",classOf[CooccurrenceColumnWrapperMapper],classOf[UserVectorSplitterMapper],   classOf[ToVectorAndPrefReducer],
          mapOutputKeyClass = classOf[VarIntWritable],mapOutputValueClass = classOf[VectorOrPrefWritable],
          classOf[VarIntWritable], classOf[VectorAndPrefsWritable],
          inputFormatClass,inputFormatClass,
          outputFormatClass,inputPath1,inputPath2,outPutPath,deleteFolder, numReduceTasks)
      }
}