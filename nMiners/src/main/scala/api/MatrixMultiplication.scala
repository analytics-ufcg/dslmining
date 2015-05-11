package api

import java.util
import java.util.{Collections, PriorityQueue}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable
import org.apache.mahout.cf.taste.impl.recommender.{ByValueRecommendedItemComparator, GenericRecommendedItem}
import org.apache.mahout.cf.taste.recommender.RecommendedItem
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math.function.Functions
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, Vector, VectorWritable}
import utils.Implicits._
import utils.MapReduceUtils

/**
Convert file at the format:
 key = item
 Value = [Vector of cooccurrence] [users who access the item] [list of preferences]
To the format:
 Key: number line
 Value: user [recommended Items]
Example:
 Text Input:

  1	{2:1.0,3:2.0,1:3.0}	[10, 20, 30]	[1.0, 1.0, 1.0]
  2	{2:1.0,3:1.0,1:1.0}	[10]	[1.0]
  3	{2:1.0,3:2.0,1:2.0}	[10, 20]	[1.0, 1.0]
  4	{4:1.0}	[40]	[1.0]

 Text Output:
   10	{2:1.0,3:1.0,1:1.0}
   20	{3:1.0,1:1.0}
   30	{1:1.0}
   40	{4:1.0}
  **/


class PartialMultiplyMapper extends Mapper[VarIntWritable,VectorAndPrefsWritable,  VarLongWritable,VectorWritable] {

  /**
   * Realize the partial matrix multiplication, the result should be a set of tuple with (userId, partialProduct)
   * @param key item
   * @param value Vector of cooccurrence
   * @param context output manager
   */
  override def  map(key: VarIntWritable, value: VectorAndPrefsWritable , context:Mapper[VarIntWritable,VectorAndPrefsWritable,  VarLongWritable,VectorWritable]#Context) = {
    val cooccurrenceColumn = value getVector
    val  userIDs = value getUserIDs
    val  prefValues = value getValues

   /* val partialProducts = prefValues.map( pref => cooccurrenceColumn.times(pref.toDouble ))*/

    for (i <- 0 until userIDs.size()){
      val userID = userIDs.get(i)
      val prefValue = prefValues.get(i)
      val partialProduct = cooccurrenceColumn.times(prefValue toDouble)
      context.write(new VarLongWritable(userID), new VectorWritable(partialProduct))
    }

  }

}

class AggregateCombiner extends Reducer[VarLongWritable,VectorWritable,  VarLongWritable,VectorWritable] {
  /**
   *
   * @param key
   * @param values
   * @param context output manager
   */
  override def reduce(key: VarLongWritable, values: java.lang.Iterable[VectorWritable] , context:Reducer[VarLongWritable,VectorWritable,  VarLongWritable,VectorWritable]#Context ) = {
    var partial:Vector = null
    values.foreach((vector: VectorWritable) => {
      partial = if (partial == null)  vector.get() else partial.plus(vector.get())
    })
    context.write(key, new VectorWritable(partial))
  }

}

/**
 * Auxiliar object
 */
object AggregateAndRecommendReducer{
  val ITEMID_INDEX_PATH: String = "itemIDIndexPath"
  val DEFAULT_NUM_RECOMMENDATIONS: Int = 10
  val NUM_RECOMMENDATIONS: String = "numRecommendations"

}

class AggregateAndRecommendReducer extends Reducer[VarLongWritable,VectorWritable, VarLongWritable,RecommendedItemsWritable] {

  /**
   * The reduce should be produce the output
   *  Key: user
   *  Value: [recommended Items]
   *
   * @param key
   * @param values
   * @param context output manager
   */
  override def reduce(key: VarLongWritable, values: java.lang.Iterable[VectorWritable] ,
                      context: Reducer[VarLongWritable,VectorWritable, VarLongWritable,RecommendedItemsWritable]#Context ) = {

    var predictions: Vector = null
    values.foreach((item) =>{
      if (predictions == null) predictions = item.get()
      else predictions.assign(item.get(), Functions.PLUS)

    })

    val recommendationVector = predictions

    val topItems: PriorityQueue[RecommendedItem] = new PriorityQueue[RecommendedItem]( AggregateAndRecommendReducer.DEFAULT_NUM_RECOMMENDATIONS + 1,
      Collections.reverseOrder(ByValueRecommendedItemComparator.getInstance()))
    
    val recommendationVectorIterator = recommendationVector.nonZeroes() iterator

    recommendationVectorIterator.foreach((element: Element) => {

      val index = element.index()
      val value =  element.get() toFloat

      if (!(value equals Float.NaN) ){
        if (topItems.size() < AggregateAndRecommendReducer.DEFAULT_NUM_RECOMMENDATIONS) {
          topItems.add(new GenericRecommendedItem(index, value))
        } else if (value > topItems.peek().getValue) {
          topItems.add(new GenericRecommendedItem(index, value))
          topItems.poll()
        }
      }

    })

    val recommendations = new util.ArrayList[RecommendedItem](topItems.size())
    recommendations.addAll(topItems)
    Collections.sort(recommendations, ByValueRecommendedItemComparator.getInstance())
    context.write(key,  new RecommendedItemsWritable(recommendations))
  }
}


object MatrixMultiplication {
  /**
   *
   * Run the Hadoop Job using PartialMultiplyMapper and AggregateAndRecommendReducer
   * @param pathToInput the input file
   * @param outPutPath the path where the output will be put
   * @param inputFormatClass the format of the input (sequential or text)
   * @param outputFormatClass the format of the output (sequential or text)
   * @param deleteFolder if the temp folder must be deleted or not
   */
  def runJob(pathToInput:String,outPutPath:String, inputFormatClass:Class[_<:FileInputFormat[_,_]],
             outputFormatClass:Class[_<:FileOutputFormat[_,_]], deleteFolder : Boolean,
             numReduceTasks : Option[Int] = None): Unit ={

    val job = MapReduceUtils.prepareJob(jobName = "Prepare", mapperClass = classOf[PartialMultiplyMapper],
      reducerClass = classOf[AggregateAndRecommendReducer], mapOutputKeyClass = classOf[VarLongWritable],
      mapOutputValueClass = classOf[VectorWritable],
      outputKeyClass = classOf[VarLongWritable], outputValueClass = classOf[RecommendedItemsWritable],
      inputFormatClass = classOf[SequenceFileInputFormat[VarIntWritable, VectorAndPrefsWritable]],
      outputFormatClass = classOf[TextOutputFormat[VarLongWritable, RecommendedItemsWritable]],
      pathToInput, outPutPath, numReduceTasks = numReduceTasks)

    val conf: Configuration = job getConfiguration()
    conf.set(AggregateAndRecommendReducer.ITEMID_INDEX_PATH, "")
    conf.setInt(AggregateAndRecommendReducer.NUM_RECOMMENDATIONS, 10)

    MapReduceUtils.deleteFolder(outPutPath, conf)
    job.waitForCompletion(true)
  }
}