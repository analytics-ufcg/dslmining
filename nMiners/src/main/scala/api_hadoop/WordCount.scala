package api_hadoop

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import utils.Implicits.{int2IntWritable, string2text}

import scala.collection.JavaConversions.asScalaIterator

class WordMap extends Mapper[LongWritable, Text, Text, IntWritable] {
  override def map(key: LongWritable, text: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context) =
    text.toString split (" ") foreach ((word: String) => context.write(word, 1))
}

class WordReduce extends Reducer[Text, IntWritable, Text, IntWritable] {
  override def reduce(word: Text, iterator: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context) =
    context.write(word, iterator.iterator().map {
      _.get
    }.sum)
}
