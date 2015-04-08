package API

import java.util.Iterator
import Utils._

import org.apache.hadoop.mapred.{Reporter, OutputCollector, Reducer, MapReduceBase}
import org.apache.mahout.math.{VarIntWritable, RandomAccessSparseVector, VectorWritable, VarLongWritable}

/**
 * Created by arthur on 06/04/15.
 */
class UserVectorToCooccurenceReduce extends MapReduceBase with Reducer[VarIntWritable,VarIntWritable,VarIntWritable,VectorWritable]{

  override def reduce(itemIndex1: VarIntWritable, itemIndex2s: Iterator[VarIntWritable], outputCollector: OutputCollector[VarIntWritable, VectorWritable], reporter: Reporter) = {

    var cooccureenceRow = new RandomAccessSparseVector(Integer MAX_VALUE, 100);
    println(itemIndex2s.isEmpty)


    itemIndex2s.foreach((item: VarIntWritable) => {
      val itemIndex2 = item.get();

      val oldValue = cooccureenceRow get (itemIndex2)
      cooccureenceRow set(itemIndex2, oldValue + 1)

    })

    outputCollector.collect(itemIndex1,new VectorWritable(cooccureenceRow))
  }

}
