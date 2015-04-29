package api

import java.io.IOException
import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.mahout.common.{RandomUtils, ClassUtils}
import org.apache.mahout.math.hadoop.similarity.cooccurrence.Vectors
import org.apache.mahout.math.map.OpenIntIntHashMap
import org.apache.mahout.math.{RandomAccessSparseVector, Vector, VectorWritable}
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.VectorSimilarityMeasure

import org.apache.hadoop.mapreduce.{Mapper, Reducer}

class RowSimilarityJob {
  val NO_THRESHOLD: Double = Double.MinValue
  val NO_FIXED_RANDOM_SEED: Long = Long.MinValue
  val SIMILARITY_CLASSNAME: String = classOf[RowSimilarityJob] + ".distributedSimilarityClassname"
  val NUMBER_OF_COLUMNS: String = classOf[RowSimilarityJob] + ".numberOfColumns"
  val MAX_SIMILARITIES_PER_ROW: String = classOf[RowSimilarityJob] + ".maxSimilaritiesPerRow"
  val EXCLUDE_SELF_SIMILARITY: String = classOf[RowSimilarityJob] + ".excludeSelfSimilarity"
  val THRESHOLD: String = classOf[RowSimilarityJob] + ".threshold"
  val NORMS_PATH: String = classOf[RowSimilarityJob] + ".normsPath"
  val MAXVALUES_PATH: String = classOf[RowSimilarityJob] + ".maxWeightsPath"
  val NUM_NON_ZERO_ENTRIES_PATH: String = classOf[RowSimilarityJob] + ".nonZeroEntriesPath"
  val DEFAULT_MAX_SIMILARITIES_PER_ROW: Int = 100
  val OBSERVATIONS_PER_COLUMN_PATH: String = classOf[RowSimilarityJob] + ".observationsPerColumnPath"
  val MAX_OBSERVATIONS_PER_ROW: String = classOf[RowSimilarityJob] + ".maxObservationsPerRow"
  val MAX_OBSERVATIONS_PER_COLUMN: String = classOf[RowSimilarityJob] + ".maxObservationsPerColumn"
  val RANDOM_SEED: String = classOf[RowSimilarityJob] + ".randomSeed"
  val DEFAULT_MAX_OBSERVATIONS_PER_ROW: Int = 500
  val DEFAULT_MAX_OBSERVATIONS_PER_COLUMN: Int = 500
  val NORM_VECTOR_MARKER: Int = Integer.MIN_VALUE
  val MAXVALUE_VECTOR_MARKER: Int = Integer.MIN_VALUE + 1
  val NUM_NON_ZERO_ENTRIES_VECTOR_MARKER: Int = Integer.MIN_VALUE + 2


  private[cooccurrence] object Counters extends Enumeration {
    type Counters = Value
    val ROWS, USED_OBSERVATIONS, NEGLECTED_OBSERVATIONS, COOCCURRENCES, PRUNED_COOCCURRENCES = Value
  }

  class VectorNormMapper extends Mapper[IntWritable, VectorWritable, IntWritable, VectorWritable] {
    var similarity: VectorSimilarityMeasure = null
    var norms: Vector = null
    var nonZeroEntries: Vector = null
    var maxValues: Vector = null
    var threshold: Double = .0
    var observationsPerColumn: OpenIntIntHashMap = null
    var maxObservationsPerRow: Int = 0
    var maxObservationsPerColumn: Int = 0
    var random: Random = null


    @throws(classOf[IOException])
    @throws(classOf[InterruptedException])
    override def setup(ctx: Mapper[IntWritable, VectorWritable, IntWritable, VectorWritable]#Context) {
      val conf: Configuration = ctx.getConfiguration
      similarity = ClassUtils.instantiateAs(conf.get(SIMILARITY_CLASSNAME), classOf[VectorSimilarityMeasure])
      norms = new RandomAccessSparseVector(Integer.MAX_VALUE)
      nonZeroEntries = new RandomAccessSparseVector(Integer.MAX_VALUE)
      maxValues = new RandomAccessSparseVector(Integer.MAX_VALUE)
      threshold = conf.get(THRESHOLD).toDouble
      observationsPerColumn = Vectors.readAsIntMap(new Path(conf.get(OBSERVATIONS_PER_COLUMN_PATH)), conf)
      maxObservationsPerRow = conf.getInt(MAX_OBSERVATIONS_PER_ROW, DEFAULT_MAX_OBSERVATIONS_PER_ROW)
      maxObservationsPerColumn = conf.getInt(MAX_OBSERVATIONS_PER_COLUMN, DEFAULT_MAX_OBSERVATIONS_PER_COLUMN)
      val seed: Long = conf.get(RANDOM_SEED).toLong
      if (seed == NO_FIXED_RANDOM_SEED) {
        random = RandomUtils.getRandom
      }
      else {
        random = RandomUtils.getRandom(seed)
      }
    }

    private def sampleDown(rowVector: Vector, ctx: Mapper[IntWritable, VectorWritable, IntWritable, VectorWritable]#Context): Vector = {
      val observationsPerRow: Int = rowVector.getNumNondefaultElements
      val rowSampleRate: Double = Math.min(maxObservationsPerRow, observationsPerRow).toDouble / observationsPerRow.toDouble
      val downsampledRow: Vector = rowVector.like
      var usedObservations: Long = 0
      var neglectedObservations: Long = 0
      import scala.collection.JavaConversions._
      for (elem <- rowVector.nonZeroes) {
        val columnCount: Int = observationsPerColumn.get(elem.index)
        val columnSampleRate: Double = Math.min(maxObservationsPerColumn, columnCount).toDouble / columnCount.toDouble
        if (random.nextDouble <= Math.min(rowSampleRate, columnSampleRate)) {
          downsampledRow.setQuick(elem.index, elem.get)
          usedObservations += 1
        }
        else {
          neglectedObservations += 1
        }
      }
      ctx.getCounter(Counters.USED_OBSERVATIONS).increment(usedObservations)
      ctx.getCounter(Counters.NEGLECTED_OBSERVATIONS).increment(neglectedObservations)
      return downsampledRow
    }

    @throws(classOf[IOException])
    @throws(classOf[InterruptedException])
    protected override def map(row: IntWritable, vectorWritable: VectorWritable, ctx: Mapper[IntWritable, VectorWritable, IntWritable, VectorWritable]#Context) {
      val sampledRowVector: Vector = sampleDown(vectorWritable.get, ctx)
      val rowVector: Vector = similarity.normalize(sampledRowVector)
      var numNonZeroEntries: Int = 0
      var maxValue: Double = Double.MinValue
      import scala.collection.JavaConversions._
      for (element <- rowVector.nonZeroes) {
        val partialColumnVector: RandomAccessSparseVector = new RandomAccessSparseVector(Integer.MAX_VALUE)
        partialColumnVector.setQuick(row.get, element.get)
        ctx.write(new IntWritable(element.index), new VectorWritable(partialColumnVector))
        numNonZeroEntries += 1
        if (maxValue < element.get) {
          maxValue = element.get
        }
      }
      if (threshold != NO_THRESHOLD) {
        nonZeroEntries.setQuick(row.get, numNonZeroEntries)
        maxValues.setQuick(row.get, maxValue)
      }
      norms.setQuick(row.get, similarity.norm(rowVector))
      ctx.getCounter(Counters.ROWS).increment(1)
    }

    @throws(classOf[IOException])
    @throws(classOf[InterruptedException])
    protected override def cleanup(ctx: Mapper[IntWritable, VectorWritable, IntWritable, VectorWritable]#Context) {
      ctx.write(new IntWritable(NORM_VECTOR_MARKER), new VectorWritable(norms))
      ctx.write(new IntWritable(NUM_NON_ZERO_ENTRIES_VECTOR_MARKER), new VectorWritable(nonZeroEntries))
      ctx.write(new IntWritable(MAXVALUE_VECTOR_MARKER), new VectorWritable(maxValues))
    }
  }

}
