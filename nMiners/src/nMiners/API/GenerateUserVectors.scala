package API
import java.util.regex.Pattern

import Utils.{MapReduceUtils, Implicits}
import org.apache.hadoop.io._
import Implicits._
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, FileOutputFormat}
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

object UserVectorGenerator{
  def runJob(inputPath: String, dirOutputName:String,inputFormatClass:Class[_<:FileInputFormat[_,_]],
             outputFormatClass:Class[_<:FileOutputFormat[_,_]],deleteFolder:Boolean): Unit ={
    MapReduceUtils.runJob("First Phase",
      classOf[WikipediaToItemPrefsMapper],
      classOf[WikipediaToUserVectorReducer],
      classOf[VarLongWritable],
      classOf[VarLongWritable],
      classOf[VarLongWritable],
      classOf[VarLongWritable],
     inputFormatClass,
     outputFormatClass,
      inputPath,
      dirOutputName,
      deleteFolder)
  }

//  def runMap(inputPath: String, dirOutputName:String,inputFormatClass:Class[_<:FileInputFormat[_,_]],
//             outputFormatClass:Class[_<:FileOutputFormat[_,_]],deleteFolder:Boolean): Unit ={
//    MapReduceUtils.runMap("First Phase",
//      classOf[WikipediaToItemPrefsMapper],
//      classOf[VarLongWritable],
//      classOf[VarLongWritable],
//      inputFormatClass,
//      outputFormatClass,
//      inputPath,
//      dirOutputName,
//      deleteFolder)
//  }
}
