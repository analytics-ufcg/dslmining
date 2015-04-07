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
/*
    Vector cooccureenceRow = new RandomAccessSparseVector(Integer.MAX_VALUE,100);
    for (VarIntWritable indexWritable2 : itemIndex2s){
      int index2 = indexWritable2.get();
      cooccureenceRow.set(index2,cooccureenceRow.get(index2)+1);
    }
    context.write(itemIndex1,new VectorWritable(cooccureenceRow));
*/
    val cooccureenceRow = new RandomAccessSparseVector(Integer MAX_VALUE, 100);
    itemIndex2s.foreach((item: VarIntWritable) => cooccureenceRow set(item.get, cooccureenceRow get(item.get  + 1)))
    outputCollector.collect(itemIndex1,new VectorWritable(cooccureenceRow))
  }

}
