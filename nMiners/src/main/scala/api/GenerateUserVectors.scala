package api
import java.util.regex.Pattern

import utils.{MapReduceUtils, Implicits}
import org.apache.hadoop.io._
import Implicits._
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, FileOutputFormat}
import org.apache.hadoop.mapreduce.{Reducer, Mapper}
import org.apache.mahout.math._

/**
Convert file at the format:
 key = Number of the line
 Value = user_id: item1 item2 ... itemN
To the format:
 Key: user
 Value: {item1:number_of_occurrence, item2:number_of_occurrence, ..., itemN:number_of_occurrence}
Example:
 Input:
   10: 1 2 3
   20: 1 3
   30: 1
   40: 4
 Text Output:
   10	{2:1.0,3:1.0,1:1.0}
   20	{3:1.0,1:1.0}
   30	{1:1.0}
   40	{4:1.0}
**/

class WikipediaToItemPrefsMapper extends Mapper[LongWritable, Text, VarLongWritable, VarLongWritable] {

  val NUMBERS = Pattern compile "(\\d+)"

  /**
   * Put the output to the format UserId, Item
   * @param key
   * @param value
   * @param context
   */
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
  /**
   *
   * Count the number of items per user
   * @param userID
   * @param itemPrefs
   * @param context
   */
  override def reduce(userID: VarLongWritable, itemPrefs: java.lang.Iterable[VarLongWritable], context:  Reducer[VarLongWritable, VarLongWritable, VarLongWritable, VectorWritable]#Context) = {
    val userVector = new RandomAccessSparseVector(Integer MAX_VALUE, 100);
    itemPrefs.foreach((item: VarLongWritable) => userVector set(item.get toInt, 1.0f))
    context write(userID, new VectorWritable(userVector))
  }
}

object UserVectorGenerator{
  /**
   *
   * Run the Hadoop Job using WikipediaToItemPrefsMapper and WikipediaToUserVectorReducer
   * @param inputPath
   * @param dirOutputName
   * @param inputFormatClass
   * @param outputFormatClass
   * @param deleteFolder
   */
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
