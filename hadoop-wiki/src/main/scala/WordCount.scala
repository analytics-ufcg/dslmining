import java.util.Iterator

import Utils._
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred._

class WordMap extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] {
  override def map(key: LongWritable, text: Text, outputCollector: OutputCollector[Text, IntWritable], reporter: Reporter) = {
    text split (" ") foreach ((w: String) => outputCollector.collect(w, 1))
  }
}

class WordReduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
  override def reduce(word: Text, iterator: Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter) = output.collect(word, iterator sum)
}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new JobConf(classOf[WordMap])
    conf setJobName "word count"

    conf setOutputKeyClass(classOf[Text])
    conf setOutputValueClass(classOf[IntWritable])

    conf setMapperClass(classOf[WordMap])
    conf setCombinerClass(classOf[WordReduce])
    conf setReducerClass(classOf[WordReduce])

    conf setInputFormat(classOf[TextInputFormat])
    conf setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])

    FileInputFormat setInputPaths(conf, args(0))
    FileOutputFormat setOutputPath(conf, args(1))

    JobClient runJob conf
  }
}
