package api
import java.util.regex.Pattern

import utils.Implicits._
import utils.MapReduceUtils
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Reducer, Mapper}
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math._

/**
 * This step computes the co-occurrence of each pair of items accessed by a user, for all users.
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
 * This step builds, for all items, a vector with its items co-occurrences and a number that counts how many times this
 * co-occurrence was occurr.
 */
class UserVectorToCooccurenceReduce extends Reducer [VarIntWritable,VarIntWritable,VarIntWritable,VectorWritable]{

  override def reduce(itemIndex1: VarIntWritable, itemIndex2s: java.lang.Iterable[VarIntWritable], context:  Reducer[VarIntWritable,VarIntWritable,VarIntWritable,VectorWritable]#Context) = {

    var cooccureenceRow = new RandomAccessSparseVector(Integer MAX_VALUE, 100);

    itemIndex2s.foreach((item: VarIntWritable) => {
      val itemIndex2 = item.get()

      val oldValue = cooccureenceRow get itemIndex2
      cooccureenceRow set(itemIndex2, oldValue + 1)

    })

    context write(itemIndex1,new VectorWritable(cooccureenceRow))
  }

}


object MatrixGenerator{

  def runJOb(inputPath: String, dirOutputName:String,inputFormatClass:Class[_<:FileInputFormat[_,_]],
             outputFormatClass:Class[_<:FileOutputFormat[_,_]],deleteFolder:Boolean): Unit ={
    MapReduceUtils.runJob(
      "Second Phase",
      classOf[UserVectorToCooccurrenceMapper],
      classOf[UserVectorToCooccurenceReduce],
      classOf[VarIntWritable],
      classOf[VarIntWritable],
      classOf[VarLongWritable],
      classOf[VectorWritable],
      inputFormatClass,
      outputFormatClass,
      inputPath,
      dirOutputName,
      deleteFolder)
  }
}


