package API

import java.util
import java.util.{Collections, PriorityQueue, Iterator}

import Utils.Implicits
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapred._
import org.apache.mahout.cf.taste.hadoop.{TasteHadoopUtils, RecommendedItemsWritable}
import org.apache.mahout.cf.taste.hadoop.item.{RecommenderJob, VectorAndPrefsWritable, VectorOrPrefWritable}
import org.apache.mahout.cf.taste.impl.common.FastIDSet
import org.apache.mahout.cf.taste.impl.recommender.{GenericRecommendedItem, ByValueRecommendedItemComparator}
import org.apache.mahout.cf.taste.recommender.RecommendedItem
import org.apache.mahout.common.HadoopUtil
import org.apache.mahout.common.iterator.FileLineIterable
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math.map.OpenIntLongHashMap
import org.apache.mahout.math.{Vector, VarLongWritable, VectorWritable}
import Implicits._

class PartialMultiplyMapper extends MapReduceBase with Mapper[IntWritable,VectorAndPrefsWritable,  VarLongWritable,VectorWritable] {

  override def  map(key: IntWritable, value: VectorAndPrefsWritable , output: OutputCollector[VarLongWritable,VectorWritable] ,reporter: Reporter) = {
    val cooccurrenceColumn = value getVector
    val /*List<Long>*/ userIDs = value getUserIDs
    val /*List<Float>*/ prefValues = value getValues

    for (i <- 0 to userIDs.size()){
      val userID = userIDs.get(i)
      val prefValue:Float = prefValues.get(i)

      val partialProduct = cooccurrenceColumn.times(prefValue)
      output.collect(new VarLongWritable(userID), new VectorWritable(partialProduct));
    }

  }

}

class AggregateCombiner extends MapReduceBase with Reducer[VarLongWritable,VectorWritable,  VarLongWritable,VectorWritable] {

  override def reduce(key: VarLongWritable, values: Iterator[VectorWritable] , output: OutputCollector[VarLongWritable,VectorWritable] ,reporter: Reporter ) = {
    var partial:Vector = null
    values.foreach((vector: VectorWritable) => {
      partial = if (partial == null)  vector.get() else partial.plus(vector.get());
    })
    output.collect(key, new VectorWritable(partial))
  }

}

//class AggregateAndRecommendReducer extends MapReduceBase with Reducer[VarLongWritable,VectorWritable, VarLongWritable,RecommendedItemsWritable] {
//  private[item] val ITEMID_INDEX_PATH: String = "itemIDIndexPath"
//  private[item] val NUM_RECOMMENDATIONS: String = "numRecommendations"
//  private[item] val DEFAULT_NUM_RECOMMENDATIONS: Int = 10
//  private var indexItemIDMap: OpenIntLongHashMap = null
//
//  override def configure (conf: JobConf)  {
//    var recommendationsPerUser = conf.getInt(NUM_RECOMMENDATIONS, DEFAULT_NUM_RECOMMENDATIONS)
//    indexItemIDMap = TasteHadoopUtils.readIDIndexMap(conf.get(ITEMID_INDEX_PATH), conf)
//
//
//  }
//
//  val recommendationsPerUser:Int = 10;
//
//  override def reduce(key: VarLongWritable, values: Iterator[VectorWritable] , output: OutputCollector[VarLongWritable,RecommendedItemsWritable] ,reporter: Reporter ) = {
//
//    var recommendationVector:Vector = null;
//    values.foreach((vectorWritable) =>{
//      recommendationVector = if (recommendationVector == null)   vectorWritable.get() else  recommendationVector.plus(vectorWritable.get());
//
//    })
//
//    val topItems: PriorityQueue[RecommendedItem] = new PriorityQueue[RecommendedItem]( recommendationsPerUser + 1,
//      Collections.reverseOrder(ByValueRecommendedItemComparator.getInstance()));
//
//
//    val recommendationVectorIterator = recommendationVector.nonZeroes() iterator
//
//    recommendationVectorIterator.foreach((element: Element) => {
//
//      val index = element.index();
//      val value =  element.get();
//
//      if (topItems.size() < recommendationsPerUser) {
//        topItems.add(new GenericRecommendedItem(indexItemIDMap.get(index), value));
//      } else if (value > topItems.peek().getValue()) {
//        topItems.add(new GenericRecommendedItem(indexItemIDMap.get(index), value));
//        topItems.poll();
//      }
//
//    })
//
//
//    var recommendations =    new util.ArrayList[RecommendedItem](topItems.size());
//    recommendations.addAll(topItems);
//    Collections.sort(recommendations, ByValueRecommendedItemComparator.getInstance());
//
//
//    output.collect(key,  new RecommendedItemsWritable(recommendations));


 // }

//}
