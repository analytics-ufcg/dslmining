package API
import Utils._

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.{Reporter, OutputCollector, Mapper, MapReduceBase}
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math.{VarIntWritable, VectorWritable, VarLongWritable}

/**
 * Created by arthur on 07/04/15.
 */
class UserVectorToCooccurrenceMapper extends MapReduceBase with Mapper[VarLongWritable,VectorWritable,VarIntWritable,VarIntWritable]{

  def map( userID : VarLongWritable, userVector:VectorWritable , output: OutputCollector[VarIntWritable, VarIntWritable], reporter: Reporter) = {

    val it  = userVector get() nonZeroes() iterator()
    it.foreach((item: Element) => {
      val it2  = userVector get() nonZeroes() iterator()
      val index1 = item.index()
      it2.foreach((item2:Element)=> {
        output.collect(index1,item2.index())
      })
    })
  }
  }
