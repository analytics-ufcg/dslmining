package dsl

import api.AggregateAndRecommendReducer
import com.typesafe.config.ConfigFactory
import dsl.job.{execute, Job}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable
import org.apache.mahout.math.{VarIntWritable, VarLongWritable, VectorWritable}
import utils.MapReduceUtils

import scala.collection.JavaConversions.asScalaIterator

object RunDsl extends App {

  //ConfigFactory load the values of main/resources/application.conf file
  val config = ConfigFactory.load()
  val dataset = config.getString("nMiners.inputTests")
  val output = config.getString("nMiners.out")

  WordCount("", "") then WordCount("", "") then execute
}