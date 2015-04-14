package API

import java.util
import java.util.{Collections, PriorityQueue}

import Utils.Implicits._
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable
import org.apache.mahout.cf.taste.impl.recommender.{ByValueRecommendedItemComparator, GenericRecommendedItem}
import org.apache.mahout.cf.taste.recommender.RecommendedItem
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math.function.Functions
import org.apache.mahout.math.map.OpenIntLongHashMap
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, Vector, VectorWritable}

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
   * @param key
   * @param value
   * @param context
   */
  override def  map(key: VarIntWritable, value: VectorAndPrefsWritable , context:Mapper[VarIntWritable,VectorAndPrefsWritable,  VarLongWritable,VectorWritable]#Context) = {
    val cooccurrenceColumn = value getVector
    val  userIDs = value getUserIDs
    val  prefValues = value getValues

    val partialProducts = prefValues.map( pref => cooccurrenceColumn.times(pref.toDouble ))

    for (i <- 0 until userIDs.size()){
      val userID = userIDs.get(i)
      val prefValue = prefValues.get(i)
      val partialProduct = cooccurrenceColumn.times(prefValue toDouble)
      context.write(new VarLongWritable(userID), new VectorWritable(partialProduct));
    }

  }

}

class AggregateCombiner extends Reducer[VarLongWritable,VectorWritable,  VarLongWritable,VectorWritable] {
  /**
   *
   * @param key
   * @param values
   * @param context
   */
  override def reduce(key: VarLongWritable, values: java.lang.Iterable[VectorWritable] , context:Reducer[VarLongWritable,VectorWritable,  VarLongWritable,VectorWritable]#Context ) = {
    var partial:Vector = null
    values.foreach((vector: VectorWritable) => {
      partial = if (partial == null)  vector.get() else partial.plus(vector.get());
    })
    context.write(key, new VectorWritable(partial))
  }

}

object AggregateAndRecommendReducer{
  val ITEMID_INDEX_PATH: String = "itemIDIndexPath"
  val DEFAULT_NUM_RECOMMENDATIONS: Int = 10
  val NUM_RECOMMENDATIONS: String = "numRecommendations"

}

class AggregateAndRecommendReducer extends Reducer[VarLongWritable,VectorWritable, VarLongWritable,RecommendedItemsWritable] {
  private var indexItemIDMap: OpenIntLongHashMap = null

  /**
   * Setup context configuration
   * @param context
   */
  override def setup (context: Reducer[VarLongWritable,VectorWritable, VarLongWritable,RecommendedItemsWritable]#Context)  {
    val conf = context.getConfiguration
    var recommendationsPerUser = conf.getInt(AggregateAndRecommendReducer.NUM_RECOMMENDATIONS, AggregateAndRecommendReducer.DEFAULT_NUM_RECOMMENDATIONS)
   // indexItemIDMap = TasteHadoopUtils.readIDIndexMap(conf.get(AggregateAndRecommendReducer.ITEMID_INDEX_PATH), conf)


  }

  val recommendationsPerUser:Int = 10;

  /**
   * The reduce should be produce the output
   *  Key: user
   *  Value: [recommended Items]
   *
   * @param key
   * @param values
   * @param context
   */
  override def reduce(key: VarLongWritable, values: java.lang.Iterable[VectorWritable] , context: Reducer[VarLongWritable,VectorWritable, VarLongWritable,RecommendedItemsWritable]#Context ) = {

    var predictions: Vector = null
    //var valores = values.toBuffer
    values.foreach((item) =>{
      println(item)
      if (predictions == null) predictions = item.get()
      else predictions.assign(item.get(), Functions.PLUS);

    })

    var recommendationVector = predictions

    //WRITE!!!!!!!!

    val topItems: PriorityQueue[RecommendedItem] = new PriorityQueue[RecommendedItem]( recommendationsPerUser + 1,
      Collections.reverseOrder(ByValueRecommendedItemComparator.getInstance()));


    val recommendationVectorIterator = recommendationVector.nonZeroes() iterator

    recommendationVectorIterator.foreach((element: Element) => {

      val index = element.index();
      val value =  element.get();

      if (topItems.size() < recommendationsPerUser) {
        topItems.add(new GenericRecommendedItem(index, value toFloat));
      } else if (value > topItems.peek().getValue()) {
        topItems.add(new GenericRecommendedItem(index, value toFloat));
        topItems.poll();
      }

    })


    var recommendations =    new util.ArrayList[RecommendedItem](topItems.size());
    recommendations.addAll(topItems);
    Collections.sort(recommendations, ByValueRecommendedItemComparator.getInstance());


    context.write(key,  new RecommendedItemsWritable(recommendations));


  }

}
