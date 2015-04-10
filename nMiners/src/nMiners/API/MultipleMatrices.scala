package API

import java.util
import java.util.{Collections, PriorityQueue, Iterator}

import Utils.Implicits
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Reducer, Mapper}
import org.apache.mahout.cf.taste.hadoop.{TasteHadoopUtils, RecommendedItemsWritable}
import org.apache.mahout.cf.taste.hadoop.item.{RecommenderJob, VectorAndPrefsWritable, VectorOrPrefWritable}
import org.apache.mahout.cf.taste.impl.common.FastIDSet
import org.apache.mahout.cf.taste.impl.recommender.{GenericRecommendedItem, ByValueRecommendedItemComparator}
import org.apache.mahout.cf.taste.recommender.RecommendedItem
import org.apache.mahout.common.HadoopUtil
import org.apache.mahout.common.iterator.FileLineIterable
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math.function.Functions
import org.apache.mahout.math.map.OpenIntLongHashMap
import org.apache.mahout.math.{VarIntWritable, Vector, VarLongWritable, VectorWritable}
import Implicits._

import scala.collection.mutable.ArrayBuffer

class PartialMultiplyMapper extends Mapper[VarIntWritable,VectorAndPrefsWritable,  VarLongWritable,VectorWritable] {

  override def  map(key: VarIntWritable, value: VectorAndPrefsWritable , context:Mapper[VarIntWritable,VectorAndPrefsWritable,  VarLongWritable,VectorWritable]#Context) = {
    val cooccurrenceColumn = value getVector
    val  userIDs = value getUserIDs
    val  prefValues = value getValues

    val partialProducts = prefValues.map( pref => cooccurrenceColumn.times(pref.toDouble ))

    for (i <- 0 until userIDs.size()){
      val userID = userIDs.get(i)
      val prefValue = prefValues.get(i)
      val partiaProduct = cooccurrenceColumn.times(prefValue toDouble)
      context.write(new VarLongWritable(userID), new VectorWritable(partiaProduct));
    }

  }

}

class AggregateCombiner extends Reducer[VarLongWritable,VectorWritable,  VarLongWritable,VectorWritable] {

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

  override def setup (context: Reducer[VarLongWritable,VectorWritable, VarLongWritable,RecommendedItemsWritable]#Context)  {
    val conf = context.getConfiguration
    var recommendationsPerUser = conf.getInt(AggregateAndRecommendReducer.NUM_RECOMMENDATIONS, AggregateAndRecommendReducer.DEFAULT_NUM_RECOMMENDATIONS)
   // indexItemIDMap = TasteHadoopUtils.readIDIndexMap(conf.get(AggregateAndRecommendReducer.ITEMID_INDEX_PATH), conf)


  }

  val recommendationsPerUser:Int = 10;


  override def reduce(key: VarLongWritable, values: java.lang.Iterable[VectorWritable] , context: Reducer[VarLongWritable,VectorWritable, VarLongWritable,RecommendedItemsWritable]#Context ) = {

    var predictions: Vector = null
    var valores = values.toBuffer
    valores.foreach((item) =>{
      if (predictions == null) predictions = item.get()
      else predictions.assign(item.get(), Functions.PLUS);

    })










//    var recommendationVector:Vector = null;
//    valores.foreach((vectorWritable) =>{
//      recommendationVector = if (recommendationVector == null)   vectorWritable.get() else  recommendationVector.plus(vectorWritable.get());
//
//    })

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
