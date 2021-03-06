package hadoop.api

import java.io.IOException
import java.util
import java.util.{Comparator, Random}

import com.google.common.primitives.Ints
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.mahout.cf.taste.hadoop.preparation.{ToItemVectorsMapper, ToItemVectorsReducer}
import org.apache.mahout.common.mapreduce.{VectorSumCombiner, VectorSumReducer}
import org.apache.mahout.common.{ClassUtils, RandomUtils}
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math._
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.{VectorSimilarityMeasure, VectorSimilarityMeasures}
import org.apache.mahout.math.hadoop.similarity.cooccurrence.{MutableElement, RowSimilarityJob, TopElementsQueue, Vectors}
import org.apache.mahout.math.map.OpenIntIntHashMap
import utils.Implicits._
import utils.MapReduceUtils


object SimilarityMatrix {
  val NO_THRESHOLD: Double = Double.MinValue
  val NO_FIXED_RANDOM_SEED: Long = Long.MinValue
  val SIMILARITY_CLASSNAME: String = SimilarityMatrix.getClass + ".distributedSimilarityClassname"
  val NUMBER_OF_COLUMNS: String = SimilarityMatrix.getClass + ".numberOfColumns"
  val MAX_SIMILARITIES_PER_ROW: String = SimilarityMatrix.getClass + ".maxSimilaritiesPerRow"
  val EXCLUDE_SELF_SIMILARITY: String = SimilarityMatrix.getClass + ".excludeSelfSimilarity"
  val THRESHOLD: String = SimilarityMatrix.getClass + ".threshold"
  val NORMS_PATH: String = SimilarityMatrix.getClass + ".normsPath"
  val MAXVALUES_PATH: String = SimilarityMatrix.getClass + ".maxWeightsPath"
  val NUM_NON_ZERO_ENTRIES_PATH: String = SimilarityMatrix.getClass + ".nonZeroEntriesPath"
  val DEFAULT_MAX_SIMILARITIES_PER_ROW: Int = 100
  val OBSERVATIONS_PER_COLUMN_PATH: String = SimilarityMatrix.getClass + ".observationsPerColumnPath"
  val MAX_OBSERVATIONS_PER_ROW: String = SimilarityMatrix.getClass + ".maxObservationsPerRow"
  val MAX_OBSERVATIONS_PER_COLUMN: String = SimilarityMatrix.getClass + ".maxObservationsPerColumn"
  val RANDOM_SEED: String = SimilarityMatrix.getClass + ".randomSeed"
  val DEFAULT_MAX_OBSERVATIONS_PER_ROW: Int = 500
  val DEFAULT_MAX_OBSERVATIONS_PER_COLUMN: Int = 500
  val NORM_VECTOR_MARKER: Int = Integer.MIN_VALUE
  val MAXVALUE_VECTOR_MARKER: Int = Integer.MIN_VALUE + 1
  val NUM_NON_ZERO_ENTRIES_VECTOR_MARKER: Int = Integer.MIN_VALUE + 2

  /**
   *
   * @param inputPath file input, must be a uservector result
   * @param outPutPath path to the output
   * @param inputFormatClass the format of the input (sequence or text file)
   * @param outputFormatClass the format of the output (sequence or text file)
   * @param deleteFolder if the temp folder must be deleted
   * @param numMapTasks
   * @param similarityClassnameArg the name of the similarity type
   * @param basePath base path for temp files
   * @param numReduceTasks max number of task who the reducer can use
   */
  def generateSimilarityMatrix(inputPath: String, outPutPath: String, inputFormatClass: Class[_ <: FileInputFormat[_, _]],
             outputFormatClass: Class[_ <: FileOutputFormat[_, _]], deleteFolder: Boolean,
             numMapTasks: Option[Int] = None, similarityClassnameArg: String, basePath: String, numReduceTasks: Option[Int]): Unit = {

    var numberOfColumns: Int = 0
    val abstractJob = new RowSimilarityJob()
    numberOfColumns = abstractJob.getDimensions(inputPath)

    var similarityClassname: String = null
    try {
      similarityClassname = VectorSimilarityMeasures.valueOf(similarityClassnameArg).getClassname
    }
    catch {
      case iae: IllegalArgumentException =>
        similarityClassname = similarityClassnameArg
    }


    val maxSimilaritiesPerRow: Int = DEFAULT_MAX_SIMILARITIES_PER_ROW
    val threshold: Double = NO_THRESHOLD
    val randomSeed: Long = NO_FIXED_RANDOM_SEED

    val maxObservationsPerRow: Int = DEFAULT_MAX_OBSERVATIONS_PER_ROW
    val maxObservationsPerColumn: Int = DEFAULT_MAX_OBSERVATIONS_PER_COLUMN

    val weightsPath: Path = new Path(basePath, "weight")
    val normsPath: Path = new Path(basePath, "norms.bin")
    val numNonZeroEntriesPath: Path = new Path(basePath, "numNonZeroEntries.bin")
    val maxValuesPath: Path = new Path(basePath, "maxValues.bin")
    val pairwiseSimilarityPath: Path = new Path(basePath, "pairwiseSimilarity")
    val observationsPerColumnPath: Path = new Path(basePath, "observationsPerColumn.bin")
    /*val currentPhase: AtomicInteger = new AtomicInteger*/

    val ratingMatrix = new Path(basePath, "rating_matrix")
    /*val itemVectorsPath: Path = new Path(basePath, "itemVectors")*/
    val countObsPath: Path = new Path(basePath, "countObs")

//======================================================================================================================================
//Generate RatingMatrix
//======================================================================================================================================

    val toItemVectors: Job = MapReduceUtils.prepareJob("toItemVectors",
      classOf[ToItemVectorsMapper],
      classOf[ToItemVectorsReducer],
      classOf[IntWritable],
      classOf[VectorWritable],
      classOf[IntWritable],
      classOf[VectorWritable],
      inputFormatClass,
      classOf[SequenceFileOutputFormat[VarIntWritable, VectorWritable]],
      inputPath,
      ratingMatrix, numReduceTasks = numReduceTasks)

    MapReduceUtils.deleteFolder(basePath, toItemVectors.getConfiguration)

    toItemVectors.setCombinerClass(classOf[ToItemVectorsReducer])
    toItemVectors.waitForCompletion(true)


//======================================================================================================================================
//Count Observations
// ======================================================================================================================================

    val countJob: Job = MapReduceUtils.prepareJob("countObs",
      classOf[CountObservationsMapper],
      classOf[SumObservationsReducer],
      classOf[NullWritable],
      classOf[VectorWritable],
      classOf[NullWritable],
      classOf[VectorWritable],
      classOf[SequenceFileInputFormat[IntWritable, VectorWritable]],
      classOf[SequenceFileOutputFormat[VarIntWritable, VectorWritable]],
      ratingMatrix + "/part-r-00000",
      countObsPath, numReduceTasks = numReduceTasks)

    countJob.setCombinerClass(classOf[VectorSumCombiner])
    countJob.getConfiguration.set(OBSERVATIONS_PER_COLUMN_PATH, observationsPerColumnPath.toString)
    countJob.setNumReduceTasks(1)
    countJob.waitForCompletion(true)

//======================================================================================================================================
//Normalize the vectors and calculate the norms of each one
// ======================================================================================================================================


    val normsAndTranspose: Job = MapReduceUtils.prepareJob("Weights",
      classOf[VectorNormMapper],
      classOf[MergeVectorsReducer],
      classOf[VarIntWritable],
      classOf[VectorWritable],
      classOf[VarIntWritable],
      classOf[VectorWritable],
      classOf[SequenceFileInputFormat[VarIntWritable, VectorWritable]],
      classOf[SequenceFileOutputFormat[VarIntWritable, VectorWritable]],
      ratingMatrix + "/part-r-00000",
      weightsPath, numReduceTasks = numReduceTasks)


    normsAndTranspose.setCombinerClass(classOf[MergeVectorsCombiner])
    val normsAndTransposeConf: Configuration = normsAndTranspose.getConfiguration
    normsAndTransposeConf.set(THRESHOLD, String.valueOf(threshold))
    normsAndTransposeConf.set(NORMS_PATH, normsPath.toString)
    normsAndTransposeConf.set(NUM_NON_ZERO_ENTRIES_PATH, numNonZeroEntriesPath.toString)
    normsAndTransposeConf.set(MAXVALUES_PATH, maxValuesPath.toString)
    normsAndTransposeConf.set(SIMILARITY_CLASSNAME, similarityClassname)
    normsAndTransposeConf.set(OBSERVATIONS_PER_COLUMN_PATH, observationsPerColumnPath.toString)
    normsAndTransposeConf.set(MAX_OBSERVATIONS_PER_ROW, String.valueOf(maxObservationsPerRow))
    normsAndTransposeConf.set(MAX_OBSERVATIONS_PER_COLUMN, String.valueOf(maxObservationsPerColumn))
    normsAndTransposeConf.set(RANDOM_SEED, String.valueOf(randomSeed))
    normsAndTranspose.waitForCompletion(true)



//======================================================================================================================================
//Calculate the similarity matrix
// ======================================================================================================================================

    val pairwiseSimilarity: Job = MapReduceUtils.prepareJob("PairWiseSimilarity",
      classOf[CooccurrencesMapper],
      classOf[SimilarityReducer],
      classOf[VarIntWritable],
      classOf[VectorWritable],
      classOf[VarIntWritable],
      classOf[VectorWritable],
      classOf[SequenceFileInputFormat[VarIntWritable, VectorWritable]],
      classOf[SequenceFileOutputFormat[VarIntWritable, VectorWritable]],
      weightsPath + "/part-r-00000",
      pairwiseSimilarityPath, numReduceTasks = numReduceTasks)

    pairwiseSimilarity.setCombinerClass(classOf[VectorSumReducer])
    val excludeSelfSimilarity = true
    val pairwiseConf: Configuration = pairwiseSimilarity.getConfiguration
    pairwiseConf.set(THRESHOLD, String.valueOf(threshold))
    pairwiseConf.set(NORMS_PATH, normsPath.toString)
    pairwiseConf.set(NUM_NON_ZERO_ENTRIES_PATH, numNonZeroEntriesPath.toString)
    pairwiseConf.set(MAXVALUES_PATH, maxValuesPath.toString)
    pairwiseConf.set(SIMILARITY_CLASSNAME, similarityClassname)
    pairwiseConf.setInt(NUMBER_OF_COLUMNS, numberOfColumns)
    pairwiseConf.setBoolean(EXCLUDE_SELF_SIMILARITY, excludeSelfSimilarity)
    pairwiseSimilarity.waitForCompletion(true)

//======================================================================================================================================
//Unsymmetrify
// ======================================================================================================================================

    val asMatrix: Job = MapReduceUtils.prepareJob("asMatrix",
      classOf[UnsymmetrifyMapper],
      classOf[MergeToTopKSimilaritiesReducer],
      classOf[VarIntWritable],
      classOf[VectorWritable],
      classOf[VarIntWritable],
      classOf[VectorWritable],
      classOf[SequenceFileInputFormat[VarIntWritable, VectorWritable]],
      outputFormatClass,
      pairwiseSimilarityPath + "/part-r-00000",
      outPutPath, numReduceTasks = numReduceTasks)


    //inputFormatClass: Class[_ <: FileInputFormat[_, _]],
    //outputFormatClass: Class[_ <: FileOutputFormat[_, _]]

    asMatrix.setCombinerClass(classOf[MergeToTopKSimilaritiesReducer])
    asMatrix.getConfiguration.setInt(MAX_SIMILARITIES_PER_ROW, maxSimilaritiesPerRow)
    /*val succeeded: Boolean = asMatrix.waitForCompletion(true)*/

  }

}

/**
 * Save the number of items of each user.
 * Input: {10: 1 2 3 // 20: 1 2 // 30: 1 // 40:4 }
 * Output: {40:1.0,20:2.0,10:3.0,30:1.0}
 */
class CountObservationsMapper extends Mapper[IntWritable, VectorWritable, NullWritable, VectorWritable] {
  var columnCounts: Vector = new RandomAccessSparseVector(Integer.MAX_VALUE)

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def map(rowIndex: IntWritable, rowVectorWritable: VectorWritable, ctx: Mapper[IntWritable, VectorWritable,
    NullWritable, VectorWritable]#Context) = {
    val row: Vector = rowVectorWritable.get()
    val i$: util.Iterator[Element] = row.nonZeroes().iterator()

    while (i$.hasNext) {
      val elem: Element = i$.next()
      this.columnCounts.setQuick(elem.index(), this.columnCounts.getQuick(elem.index()) + 1.0D)
    }
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def cleanup(ctx: Mapper[IntWritable, VectorWritable, NullWritable, VectorWritable]#Context) = {
    ctx.write(NullWritable.get, new VectorWritable(columnCounts))
  }

}

class SumObservationsReducer extends Reducer[NullWritable, VectorWritable, NullWritable, VectorWritable] {
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def reduce(nullWritable: NullWritable, partialVectors: java.lang.Iterable[VectorWritable], ctx: Reducer[NullWritable, VectorWritable, NullWritable, VectorWritable]#Context) {
    val counts: Vector = Vectors.sum(partialVectors.iterator())
    Vectors.write(counts, new Path(ctx.getConfiguration.get(SimilarityMatrix.OBSERVATIONS_PER_COLUMN_PATH)), ctx.getConfiguration)
  }
}


/**
 * Normalize the vectors and calculate the norms of each one
 * Input: Item: {user:value} ======> {1: {20:1, 10:1, 30:1},  2: {10:1}}
 * Output: user: {item:valueNormalized} ======>  {10: {1:1, 2:1, 3:1}, 20:{2:1}}
 */
class VectorNormMapper extends Mapper[IntWritable, VectorWritable, VarIntWritable, VectorWritable]() {
  var similarity: VectorSimilarityMeasure = null
  var norms: Vector = null
  var nonZeroEntries: Vector = null
  var maxValues: Vector = null
  var threshold: Double = .0
  var observationsPerColumn: OpenIntIntHashMap = null
  var maxObservationsPerRow: Int = 0
  var maxObservationsPerColumn: Int = 0
  var random: Random = null

  def VectorNormMapper() {
    println("\nNo last name or age given.")
  }

  override def setup(ctx: Mapper[IntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    val conf: Configuration = ctx.getConfiguration
    similarity = ClassUtils.instantiateAs(conf.get(SimilarityMatrix.SIMILARITY_CLASSNAME), classOf[VectorSimilarityMeasure])
    norms = new RandomAccessSparseVector(Integer.MAX_VALUE)
    nonZeroEntries = new RandomAccessSparseVector(Integer.MAX_VALUE)
    maxValues = new RandomAccessSparseVector(Integer.MAX_VALUE)
    threshold = conf.get(SimilarityMatrix.THRESHOLD).toDouble
    observationsPerColumn = Vectors.readAsIntMap(new Path(conf.get(SimilarityMatrix.OBSERVATIONS_PER_COLUMN_PATH)), conf)
    maxObservationsPerRow = conf.getInt(SimilarityMatrix.MAX_OBSERVATIONS_PER_ROW, SimilarityMatrix.DEFAULT_MAX_OBSERVATIONS_PER_ROW)
    maxObservationsPerColumn = conf.getInt(SimilarityMatrix.MAX_OBSERVATIONS_PER_COLUMN, SimilarityMatrix.DEFAULT_MAX_OBSERVATIONS_PER_COLUMN)
    val seed: Long = conf.get(SimilarityMatrix.RANDOM_SEED).toLong
    if (seed == SimilarityMatrix.NO_FIXED_RANDOM_SEED) {
      random = RandomUtils.getRandom
    }
    else {
      random = RandomUtils.getRandom(seed)
    }
  }

  private def sampleDown(rowVector: Vector, ctx: Mapper[IntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context): Vector = {
    val observationsPerRow: Int = rowVector.getNumNondefaultElements
    val rowSampleRate: Double = Math.min(maxObservationsPerRow, observationsPerRow).toDouble / observationsPerRow.toDouble
    val downsampledRow: Vector = rowVector.like
    var usedObservations: Long = 0
    var neglectedObservations: Long = 0
    for (elem <- rowVector.nonZeroes.iterator()) {
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
    downsampledRow
  }

  @throws(classOf[InterruptedException])
  protected override def map(row: IntWritable, vectorWritable: VectorWritable, ctx: Mapper[IntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    val sampledRowVector: Vector = sampleDown(vectorWritable.get, ctx)
    val rowVector: Vector = similarity.normalize(sampledRowVector)
    var numNonZeroEntries: Int = 0
    var maxValue: Double = Double.MinValue
    //      import scala.collection.JavaConversions._
    for (element <- rowVector.nonZeroes.iterator()) {
      val partialColumnVector: RandomAccessSparseVector = new RandomAccessSparseVector(Integer.MAX_VALUE)
      partialColumnVector.setQuick(row.get, element.get)
      ctx.write(new VarIntWritable(element.index), new VectorWritable(partialColumnVector))
      numNonZeroEntries += 1
      if (maxValue < element.get) {
        maxValue = element.get
      }
    }
    if (threshold != SimilarityMatrix.NO_THRESHOLD) {
      nonZeroEntries.setQuick(row.get, numNonZeroEntries)
      maxValues.setQuick(row.get, maxValue)
    }
    norms.setQuick(row.get, similarity.norm(rowVector))
  }

  protected override def cleanup(ctx: Mapper[IntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    ctx.write(new VarIntWritable(SimilarityMatrix.NORM_VECTOR_MARKER), new VectorWritable(norms))
    ctx.write(new VarIntWritable(SimilarityMatrix.NUM_NON_ZERO_ENTRIES_VECTOR_MARKER), new VectorWritable(nonZeroEntries))
    ctx.write(new VarIntWritable(SimilarityMatrix.MAXVALUE_VECTOR_MARKER), new VectorWritable(maxValues))
  }
}


class MergeVectorsCombiner extends Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable] {
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def reduce(row: VarIntWritable, partialVectors: java.lang.Iterable[VectorWritable], ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) = {
    ctx.write(row, new VectorWritable(Vectors.merge(partialVectors)))
  }
}

class MergeVectorsReducer extends Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable] {
  var normsPath: Path = null
  var numNonZeroEntriesPath: Path = null
  var maxValuesPath: Path = null

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def setup(ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    normsPath = new Path(ctx.getConfiguration.get(SimilarityMatrix.NORMS_PATH))
    numNonZeroEntriesPath = new Path(ctx.getConfiguration.get(SimilarityMatrix.NUM_NON_ZERO_ENTRIES_PATH))
    maxValuesPath = new Path(ctx.getConfiguration.get(SimilarityMatrix.MAXVALUES_PATH))
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def reduce(row: VarIntWritable, partialVectors: java.lang.Iterable[VectorWritable], ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    val partialVector: Vector = Vectors.merge(partialVectors)
    if (row.get == SimilarityMatrix.NORM_VECTOR_MARKER) {
      Vectors.write(partialVector, normsPath, ctx.getConfiguration)
    }
    else if (row.get == SimilarityMatrix.MAXVALUE_VECTOR_MARKER) {
      Vectors.write(partialVector, maxValuesPath, ctx.getConfiguration)
    }
    else if (row.get == SimilarityMatrix.NUM_NON_ZERO_ENTRIES_VECTOR_MARKER) {
      Vectors.write(partialVector, numNonZeroEntriesPath, ctx.getConfiguration, true)
    }
    else {
      ctx.write(row, new VectorWritable(partialVector))
    }
  }
}

/**
  * Calculate the similarity matrix
 */
class CooccurrencesMapper extends Mapper[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable] {
  var similarity: VectorSimilarityMeasure = null
  var numNonZeroEntries: OpenIntIntHashMap = null
  var maxValues: Vector = null
  var threshold: Double = .0

  val BY_INDEX: Comparator[Vector.Element] = new Comparator[Element] {
    override def compare(one: Element, two: Element): Int = {
      Ints.compare(one.index, two.index)
    }
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def setup(ctx: Mapper[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    similarity = ClassUtils.instantiateAs(ctx.getConfiguration.get(SimilarityMatrix.SIMILARITY_CLASSNAME), classOf[VectorSimilarityMeasure])
    numNonZeroEntries = Vectors.readAsIntMap(new Path(ctx.getConfiguration.get(SimilarityMatrix.NUM_NON_ZERO_ENTRIES_PATH)), ctx.getConfiguration)
    maxValues = Vectors.read(new Path(ctx.getConfiguration.get(SimilarityMatrix.MAXVALUES_PATH)), ctx.getConfiguration)
    threshold = ctx.getConfiguration.get(SimilarityMatrix.THRESHOLD).toDouble
  }

  private def consider(occurrenceA: Vector.Element, occurrenceB: Vector.Element): Boolean = {
    val numNonZeroEntriesA: Int = numNonZeroEntries.get(occurrenceA.index)
    val numNonZeroEntriesB: Int = numNonZeroEntries.get(occurrenceB.index)
    val maxValueA: Double = maxValues.get(occurrenceA.index)
    val maxValueB: Double = maxValues.get(occurrenceB.index)
    similarity.consider(numNonZeroEntriesA, numNonZeroEntriesB, maxValueA, maxValueB, threshold)
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def map(column: VarIntWritable, occurrenceVector: VectorWritable, ctx: Mapper[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    val occurrences: Array[Vector.Element] = Vectors.toArray(occurrenceVector)
    util.Arrays.sort(occurrences, BY_INDEX)
    var cooccurrences: Int = 0
    var prunedCooccurrences: Int = 0
    for (n: Int <- 0 until occurrences.length) {
      val occurrenceA: Vector.Element = occurrences(n)
      val dots: Vector = new RandomAccessSparseVector(Integer.MAX_VALUE)
      for (m: Int <- n until occurrences.length) {
        val occurrenceB: Vector.Element = occurrences(m)
        if (threshold == SimilarityMatrix.NO_THRESHOLD || consider(occurrenceA, occurrenceB)) {
          dots.setQuick(occurrenceB.index(), similarity.aggregate(occurrenceA.get(), occurrenceB.get()))
          cooccurrences = cooccurrences + 1
        } else {
          prunedCooccurrences = prunedCooccurrences + 1
        }
      }
      ctx.write(new VarIntWritable(occurrenceA.index()), new VectorWritable(dots))
    }
  }
}

class SimilarityReducer extends Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable] {
  private var similarity: VectorSimilarityMeasure = null
  private var numberOfColumns: Int = 0
  private var excludeSelfSimilarity: Boolean = false
  private var norms: Vector = null
  private var treshold: Double = .0

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def setup(ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    similarity = ClassUtils.instantiateAs(ctx.getConfiguration.get(SimilarityMatrix.SIMILARITY_CLASSNAME), classOf[VectorSimilarityMeasure])
    numberOfColumns = ctx.getConfiguration.getInt(SimilarityMatrix.NUMBER_OF_COLUMNS, -1)
    excludeSelfSimilarity = ctx.getConfiguration.getBoolean(SimilarityMatrix.EXCLUDE_SELF_SIMILARITY, false)
    norms = Vectors.read(new Path(ctx.getConfiguration.get(SimilarityMatrix.NORMS_PATH)), ctx.getConfiguration)
    treshold = ctx.getConfiguration.get(SimilarityMatrix.THRESHOLD).toDouble
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def reduce(row: VarIntWritable, partialDots: java.lang.Iterable[VectorWritable], ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    val partialDotsIterator: Iterator[VectorWritable] = partialDots.iterator
    val dots: Vector = partialDotsIterator.next().get
    while (partialDotsIterator.hasNext) {
      val toAdd: Vector = partialDotsIterator.next().get
      for (nonZeroElement <- toAdd.nonZeroes) {
        dots.setQuick(nonZeroElement.index, dots.getQuick(nonZeroElement.index) + nonZeroElement.get)
      }
    }
    val similarities: Vector = dots.like
    val normA: Double = norms.getQuick(row.get)
    for (b <- dots.nonZeroes.iterator()) {
      val similarityValue: Double = similarity.similarity(b.get, normA, norms.getQuick(b.index), numberOfColumns)
      if (similarityValue >= treshold) {
        similarities.set(b.index, similarityValue)
      }
    }
    if (excludeSelfSimilarity) {
      similarities.setQuick(row.get, 0)
    }
    ctx.write(row, new VectorWritable(similarities))
  }
}


class UnsymmetrifyMapper extends Mapper[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable] {
  private var maxSimilaritiesPerRow: Int = 0

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def setup(ctx: Mapper[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    maxSimilaritiesPerRow = ctx.getConfiguration.getInt(SimilarityMatrix.MAX_SIMILARITIES_PER_ROW, 0)
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def map(row: VarIntWritable, similaritiesWritable: VectorWritable, ctx: Mapper[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    val similarities: Vector = similaritiesWritable.get
    val transposedPartial: Vector = new RandomAccessSparseVector(similarities.size, 1)
    val topKQueue: TopElementsQueue = new TopElementsQueue(maxSimilaritiesPerRow)
    for (nonZeroElement <- similarities.nonZeroes.iterator()) {
      val top: MutableElement = topKQueue.top
      val candidateValue: Double = nonZeroElement.get
      if (candidateValue > top.get) {
        top.setIndex(nonZeroElement.index)
        top.set(candidateValue)
        topKQueue.updateTop()
      }
      transposedPartial.setQuick(row.get, candidateValue)
      ctx.write(new VarIntWritable(nonZeroElement.index), new VectorWritable(transposedPartial))
      transposedPartial.setQuick(row.get, 0.0)
    }
    val topKSimilarities: Vector = new RandomAccessSparseVector(similarities.size, maxSimilaritiesPerRow)
    import scala.collection.JavaConversions._
    for (topKSimilarity <- topKQueue.getTopElements) {
      topKSimilarities.setQuick(topKSimilarity.index, topKSimilarity.get)
    }
    ctx.write(row, new VectorWritable(topKSimilarities))
  }
}

class MergeToTopKSimilaritiesReducer extends Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable] {
  private var maxSimilaritiesPerRow: Int = 0

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def setup(ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    maxSimilaritiesPerRow = ctx.getConfiguration.getInt(SimilarityMatrix.MAX_SIMILARITIES_PER_ROW, 0)
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def reduce(row: VarIntWritable, partials: java.lang.Iterable[VectorWritable], ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    val allSimilarities: Vector = Vectors.merge(partials)
    val topKSimilarities: Vector = Vectors.topKElements(maxSimilaritiesPerRow, allSimilarities)
    ctx.write(row, new VectorWritable(topKSimilarities))
  }
}