package API
import java.util.regex.Pattern

import Utils.Implicits._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.{Reducer, Mapper}
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math._

/**
 * Created by arthur on 07/04/15.
 */
class UserVectorToCooccurrenceMapper extends Mapper [VarLongWritable, VectorWritable, VarIntWritable, VarIntWritable]{

  val NUMBERS = Pattern compile "(\\d+)"

  override def map( userID : VarLongWritable, userVector:VectorWritable , context: Mapper[VarLongWritable,VectorWritable,VarIntWritable,VarIntWritable]#Context) = {
    val it  = userVector get() nonZeroes() iterator()
    it.foreach((item: Element) => {
      val it2  = userVector get() nonZeroes() iterator()
      val index1 = item.index()
      it2.foreach((item2:Element)=> {
        context write (index1,item2.index())
      })
    })
  }
}

/**
 * Created by arthur on 06/04/15.
 */
class UserVectorToCooccurenceReduce extends Reducer [VarIntWritable,VarIntWritable,VarIntWritable,VectorWritable]{

  override def reduce(itemIndex1: VarIntWritable, itemIndex2s: java.lang.Iterable[VarIntWritable], context:  Reducer[VarIntWritable,VarIntWritable,VarIntWritable,VectorWritable]#Context) = {

    var cooccureenceRow = new RandomAccessSparseVector(Integer MAX_VALUE, 100);
    println(itemIndex2s.isEmpty)


    itemIndex2s.foreach((item: VarIntWritable) => {
      val itemIndex2 = item.get()

      val oldValue = cooccureenceRow get itemIndex2
      cooccureenceRow set(itemIndex2, oldValue + 1)

    })

    context write(itemIndex1,new VectorWritable(cooccureenceRow))
  }

}


