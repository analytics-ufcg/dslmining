package hadoop.api

import java.util.regex.Pattern

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import utils.Implicits._
import utils.MapReduceUtils._

class WikipediaToCSVMapper extends Mapper[LongWritable, Text, Text, Text] {

  val NUMBERS = Pattern compile "(\\d+)"

  /**
   * Put the output to the format UserId, Item
   * @param key number of the line
   * @param value user_id: list of items
   * @param context output manager
   */
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context) = {

    val m = NUMBERS matcher value

    val allNumbers = m.toIterable.toList

    val userId = allNumbers.head

    allNumbers.tail.foreach { itemId =>
      context write(s"$userId,$itemId", "")
    }
  }
}

object WikipediaToCSV {
  def runJob(inputPath: String, dirOutputName: String, deleteFolder: Boolean, numReduceTasks: Option[Int]) ={
    runMap(jobName = "WikipediaToCSVMapper", mapperClass = classOf[WikipediaToCSVMapper],
      outputKeyClass = classOf[Text], outputValueClass = classOf[Text],
      classOf[TextInputFormat], classOf[TextOutputFormat[Text, Text]],
      inputPath, dirOutputName, deleteFolder, numReduceTasks)
  }

}