package api

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.mahout.math._
import utils.Implicits._
import utils.MapReduceUtils


/**
  * This step computes the co-occurrence of each pair of items accessed by a user, for all users.
 */
class UserVectorToCooccurrenceMapper extends Mapper [VarLongWritable, VectorWritable, VarIntWritable, VarIntWritable]{

  override def map( userID : VarLongWritable, userVector:VectorWritable , context: Mapper[VarLongWritable,VectorWritable,VarIntWritable,VarIntWritable]#Context) = {
    val it = (userVector get() nonZeroes() iterator()) map{_.index} toList
    val pairs = for(u1 <- it; u2 <- it if u1 != u2) yield (u1, u2)
    pairs foreach {t => context write (t._1, t._2)}

//    val it  = userVector get() nonZeroes() iterator()
//    it.foreach((item) => {
//      val it2  = userVector get() nonZeroes() iterator()
//      val index1 = item.index()
//      it2.foreach((item2)=> {
//        context write (index1,item2.index())
//      })
//    })
  }
}

/**
 * This step builds, for all items, a vector with its items co-occurrences and a number that counts how many times this
 * co-occurrence was occurr.
 */
class UserVectorToCooccurenceReduce extends Reducer [VarIntWritable,VarIntWritable,VarIntWritable,VectorWritable]{

  override def reduce(itemIndex1: VarIntWritable, itemIndex2s: java.lang.Iterable[VarIntWritable], context:  Reducer[VarIntWritable,VarIntWritable,VarIntWritable,VectorWritable]#Context) = {


    var cooccureenceRow = new RandomAccessSparseVector(Integer MAX_VALUE, 100);
    itemIndex2s.map(_.get()).toList.groupBy(identity).mapValues(_.size).filter(_._2 > 1).foreach{t => cooccureenceRow set (t._1, t._2)}

//    itemIndex2s.foreach((item: VarIntWritable) => {
//      val itemIndex2 = item.get()
//
//      val oldValue = cooccureenceRow get itemIndex2
//      cooccureenceRow set(itemIndex2, oldValue + 1)
//
//    })

    context write(itemIndex1,new VectorWritable(cooccureenceRow))
  }

}


object MatrixGenerator{

  def runJob(inputPath: String, dirOutputName:String,inputFormatClass:Class[_<:FileInputFormat[_,_]],
             outputFormatClass:Class[_<:FileOutputFormat[_,_]],deleteFolder:Boolean,numReduceTasks:Option[Int]): Unit ={
    MapReduceUtils.runJob(
      "Second Phase",
      classOf[UserVectorToCooccurrenceMapper],
      classOf[UserVectorToCooccurenceReduce],
      classOf[VarIntWritable],
      classOf[VarIntWritable],
      classOf[VarIntWritable],
      classOf[VectorWritable],
      inputFormatClass,
      outputFormatClass,
      inputPath,
      dirOutputName,
      deleteFolder)
  }
}


