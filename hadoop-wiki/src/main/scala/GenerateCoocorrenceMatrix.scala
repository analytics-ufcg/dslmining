import java.util.Iterator
import java.util.regex.Pattern

import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import Utils._
import org.apache.mahout.math._

class UserVectorToCooccurrenceMapper extends MapReduceBase with Mapper[LongWritable, Text, VarLongWritable, VarLongWritable] {

  val NUMBERS = Pattern compile "(\\d+)"

  def map(key: LongWritable, value: Text, output: OutputCollector[VarLongWritable, VarLongWritable], reporter: Reporter) = {

    val m = NUMBERS matcher value
    m find
    val userID = new VarLongWritable(m.group toLong)
    val itemID = new VarLongWritable()
    m foreach (item => {
      itemID.set(item toLong);
      output.collect(userID, itemID);
    })
  }
}

class UserVectorToCooccurrenceReducer extends MapReduceBase with Reducer[VarLongWritable, VarLongWritable, VarLongWritable, VectorWritable] {

  override def reduce(userID: VarLongWritable, itemPrefs: Iterator[VarLongWritable], outputCollector: OutputCollector[VarLongWritable, VectorWritable], reporter: Reporter) = {
    val userVector = new RandomAccessSparseVector(Integer MAX_VALUE, 100);
    itemPrefs.forEachRemaining((item: VarLongWritable) => userVector set(item.get toInt, 1.0f))
    outputCollector.collect(userID, new VectorWritable(userVector))
  }
}

object GenerateCoocorrenceMatrix {
  def main(args: Array[String]): Unit = {
    val conf = new JobConf(classOf[UserVectorToCooccurrenceMapper])
    conf setJobName "wiki parser"

    conf setOutputKeyClass classOf[VarLongWritable]
    conf setOutputValueClass classOf[VarLongWritable]

    conf setMapperClass classOf[UserVectorToCooccurrenceMapper]
    conf setReducerClass classOf[UserVectorToCooccurrenceReducer]

    conf setInputFormat classOf[TextInputFormat]
    conf setOutputFormat classOf[TextOutputFormat[VarLongWritable, VectorWritable]]

    conf setJar "hadoop.jar"
    conf setCompressMapOutput true

    FileInputFormat setInputPaths(conf, args(0))
    FileOutputFormat setOutputPath(conf, args(1))

    JobClient runJob conf
  }
}