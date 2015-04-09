package API

import java.util.Iterator
import Utils.Implicits

import org.apache.hadoop.mapreduce.{Reducer, Mapper}
//import org.apache.hadoop.mapred._
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, VectorWritable, Vector}
import Implicits.javaIterator2ElementIterator
/**
 * Created by andryw on 06/04/15.
 */

class CooccurrenceColumnWrapperMapper extends Mapper[VarIntWritable,VectorWritable,  VarIntWritable,VectorOrPrefWritable] {

  override def  map(key: VarIntWritable, value: VectorWritable , context:Mapper[VarIntWritable,VectorWritable,  VarIntWritable,VectorOrPrefWritable]#Context) = {
    context write (key, new VectorOrPrefWritable(value.get()))
  }

}

//class CooccurrenceColumnWrapperReducer extends MapReduceBase with Reducer[VarIntWritable,VectorOrPrefWritable,  VarIntWritable,Iterator[VectorOrPrefWritable]] {
//
//  override def reduce(key: VarIntWritable, value: Iterator[VectorOrPrefWritable] ,
//                      output: OutputCollector[VarIntWritable,Iterator[VectorOrPrefWritable]] ,reporter: Reporter ) = {
//    output.collect(key, value)
//  }
//
//}



class UserVectorSplitterMapper extends Mapper[VarLongWritable,VectorWritable,  VarIntWritable,VectorOrPrefWritable] {
  override def map(key: VarLongWritable , value:VectorWritable,  context:Mapper[VarLongWritable,VectorWritable,  VarIntWritable,VectorOrPrefWritable]#Context) = {
    val userID:Long = key get
    val userVector:Vector = value get
    val it = userVector.nonZeroes() iterator
    val itemIndexWritable:VarIntWritable = new VarIntWritable()
    it.foreach((item: Element) => {
      userVector set(item.get toInt, 1.0f)
      val e: Vector.Element = it.next();
      val itemIndex = e.index();
      val preferenceValue:Double = e get;
      itemIndexWritable.set(itemIndex);
      context write(itemIndexWritable,  new VectorOrPrefWritable(userID,preferenceValue toFloat))
    })
  }
}
//
//class UserVectorSplitterReducer extends MapReduceBase with Reducer[VarIntWritable,VectorOrPrefWritable,  VarIntWritable,Iterator[VectorOrPrefWritable]] {
//
//  override def reduce(key: VarIntWritable, value: Iterator[VectorOrPrefWritable] ,
//                      output: OutputCollector[VarIntWritable,Iterator[VectorOrPrefWritable]] ,reporter: Reporter ) = {
//    output.collect(key, value)
//  }
//
//}