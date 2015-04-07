package API


import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapred._
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math.{VarLongWritable, VectorWritable,Vector}
import Utils.javaIterator2ElementIterator
/**
 * Created by andryw on 06/04/15.
 */
object PrepareMatrixMultiplication {

  class CooccurrenceColumnWrapperMapper extends MapReduceBase with Mapper[IntWritable,VectorWritable,  IntWritable,VectorOrPrefWritable] {

    override def  map(key: IntWritable, value: VectorWritable , output: OutputCollector[IntWritable, VectorOrPrefWritable] ,reporter: Reporter) = {
      output.collect(key, new VectorOrPrefWritable(value.get()))
    }

  }

//  class CooccurrenceColumnWrapperReducer extends MapReduceBase with Reducer[IntWritable,VectorOrPrefWritable,  IntWritable,VectorOrPrefWritable] {
//
//    override def reduce(key: IntWritable, value: Iterator[VectorOrPrefWritable] ,
//                        output: OutputCollector[IntWritable,Iterator[VectorOrPrefWritable]] ,reporter: Reporter ) = {
//      output.collect(key, value)
//    }
//
//  }



  class UserVectorSplitterMapper extends MapReduceBase with Mapper[VarLongWritable,VectorWritable,  IntWritable,VectorOrPrefWritable] {
    def map(key: VarLongWritable , value:VectorWritable,  output: OutputCollector[IntWritable,VectorOrPrefWritable] ,reporter: Reporter) = {
      val userID:Long = key get
      val userVector:Vector = value get
      val it = userVector.nonZeroes() iterator
      val itemIndexWritable:IntWritable = new IntWritable()
      it.foreach((item: Element) => {
        userVector set(item.get toInt, 1.0f)
        val e: Vector.Element = it.next();
        val itemIndex = e.index();
        val preferenceValue:Double = e get;
        itemIndexWritable.set(itemIndex);
        output.collect(itemIndexWritable,  new VectorOrPrefWritable(userID,preferenceValue toFloat))
      })


    }
  }

}
