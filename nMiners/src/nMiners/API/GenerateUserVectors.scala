package API
import java.util.regex.Pattern

import Utils.Implicits
import org.apache.hadoop.io._
import Implicits._
import org.apache.hadoop.mapreduce.{Reducer, Mapper}
import org.apache.mahout.math._



class WikipediaToItemPrefsMapper extends Mapper[LongWritable, Text, VarLongWritable, VarLongWritable] {

  val NUMBERS = Pattern compile "(\\d+)"

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable,Text,VarLongWritable,VarLongWritable]#Context) = {

    val m = NUMBERS matcher value
    m find
    val userID = new VarLongWritable(m.group toLong)
    val itemID = new VarLongWritable()
    while (m find){
      itemID.set(m group() toLong);
      context write (userID, itemID);

    }
  }
}

class WikipediaToUserVectorReducer extends Reducer[VarLongWritable, VarLongWritable, VarLongWritable, VectorWritable] {

  override def reduce(userID: VarLongWritable, itemPrefs: java.lang.Iterable[VarLongWritable], context:  Reducer[VarLongWritable, VarLongWritable, VarLongWritable, VectorWritable]#Context) = {
    val userVector = new RandomAccessSparseVector(Integer MAX_VALUE, 100);
    itemPrefs.foreach((item: VarLongWritable) => userVector set(item.get toInt, 1.0f))
    context write(userID, new VectorWritable(userVector))
  }
}
