package api

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Arrays, Comparator, Random}

import com.google.common.primitives.Ints
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, IntWritable}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, FileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.mahout.common.{AbstractJob, ClassUtils, RandomUtils}
import org.apache.mahout.math.Vector.Element
import org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.{VectorSimilarityMeasure, VectorSimilarityMeasures}
import org.apache.mahout.math.hadoop.similarity.cooccurrence.{MutableElement, TopElementsQueue, Vectors}
import org.apache.mahout.math.map.OpenIntIntHashMap
import org.apache.mahout.math._
import utils.Implicits._
import utils.MapReduceUtils

class CountObservationsMapper extends Mapper[IntWritable, VectorWritable, NullWritable, VectorWritable] {
  private var columnCounts: Vector = new RandomAccessSparseVector(Integer.MAX_VALUE)

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def map(rowIndex: IntWritable, rowVectorWritable: VectorWritable, ctx: Mapper[IntWritable, VectorWritable, NullWritable, VectorWritable]#Context) {
    val row: Vector = rowVectorWritable.get
    for (elem <- row.nonZeroes) {
      columnCounts.setQuick(elem.index, columnCounts.getQuick(elem.index) + 1)
    }
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def cleanup(ctx: Mapper[IntWritable, VectorWritable, NullWritable, VectorWritable]#Context) {
    ctx.write(NullWritable.get, new VectorWritable(columnCounts))
  }
}

class SumObservationsReducer extends Reducer[NullWritable, VectorWritable, NullWritable, VectorWritable] {
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def reduce(nullWritable: NullWritable, partialVectors: java.lang.Iterable[VectorWritable], ctx: Reducer[NullWritable, VectorWritable, NullWritable, VectorWritable]#Context) {
    val counts: Vector = Vectors.sum(partialVectors.iterator())
    Vectors.write(counts, new Path(ctx.getConfiguration.get(RowSimilarityJobAnalytics.OBSERVATIONS_PER_COLUMN_PATH)), ctx.getConfiguration)
  }
}

class VectorNormMapper extends Mapper[VarLongWritable, VectorWritable, VarIntWritable, VectorWritable]() {
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

  override def setup(ctx: Mapper[VarLongWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    val conf: Configuration = ctx.getConfiguration
    similarity = ClassUtils.instantiateAs(conf.get(RowSimilarityJobAnalytics.SIMILARITY_CLASSNAME), classOf[VectorSimilarityMeasure])
    norms = new RandomAccessSparseVector(Integer.MAX_VALUE)
    nonZeroEntries = new RandomAccessSparseVector(Integer.MAX_VALUE)
    maxValues = new RandomAccessSparseVector(Integer.MAX_VALUE)
    threshold = conf.get(RowSimilarityJobAnalytics.THRESHOLD).toDouble
    observationsPerColumn = Vectors.readAsIntMap(new Path(conf.get(RowSimilarityJobAnalytics.OBSERVATIONS_PER_COLUMN_PATH)), conf)
    maxObservationsPerRow = conf.getInt(RowSimilarityJobAnalytics.MAX_OBSERVATIONS_PER_ROW, RowSimilarityJobAnalytics.DEFAULT_MAX_OBSERVATIONS_PER_ROW)
    maxObservationsPerColumn = conf.getInt(RowSimilarityJobAnalytics.MAX_OBSERVATIONS_PER_COLUMN, RowSimilarityJobAnalytics.DEFAULT_MAX_OBSERVATIONS_PER_COLUMN)
    val seed: Long = conf.get(RowSimilarityJobAnalytics.RANDOM_SEED).toLong
    if (seed == RowSimilarityJobAnalytics.NO_FIXED_RANDOM_SEED) {
      random = RandomUtils.getRandom
    }
    else {
      random = RandomUtils.getRandom(seed)
    }
  }

  private def sampleDown(rowVector: Vector, ctx: Mapper[VarLongWritable, VectorWritable, VarIntWritable, VectorWritable]#Context): Vector = {
    val observationsPerRow: Int = rowVector.getNumNondefaultElements
    val rowSampleRate: Double = Math.min(maxObservationsPerRow, observationsPerRow).toDouble / observationsPerRow.toDouble
    val downsampledRow: Vector = rowVector.like
    var usedObservations: Long = 0
    var neglectedObservations: Long = 0
    //      import scala.collection.JavaConversions._
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
    /*   ctx.getCounter(Counters.USED_OBSERVATIONS).increment(usedObservations)
    ctx.getCounter(Counters.NEGLECTED_OBSERVATIONS).increment(neglectedObservations)*/
    return downsampledRow
  }

  @throws(classOf[InterruptedException])
  protected override def map(row: VarLongWritable, vectorWritable: VectorWritable, ctx: Mapper[VarLongWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    val sampledRowVector: Vector = sampleDown(vectorWritable.get, ctx)
    val rowVector: Vector = similarity.normalize(sampledRowVector)
    var numNonZeroEntries: Int = 0
    var maxValue: Double = Double.MinValue
    //      import scala.collection.JavaConversions._
    for (element <- rowVector.nonZeroes) {
      val partialColumnVector: RandomAccessSparseVector = new RandomAccessSparseVector(Integer.MAX_VALUE)
      partialColumnVector.setQuick(row.get toInt, element.get)
      ctx.write(new VarIntWritable(element.index), new VectorWritable(partialColumnVector))
      numNonZeroEntries += 1
      if (maxValue < element.get) {
        maxValue = element.get
      }
    }
    if (threshold != RowSimilarityJobAnalytics.NO_THRESHOLD) {
      nonZeroEntries.setQuick(row.get toInt, numNonZeroEntries)
      maxValues.setQuick(row.get toInt, maxValue)
    }
    norms.setQuick(row.get toInt, similarity.norm(rowVector))
    //      ctx.getCounter(Counters.ROWS).increment(1)
  }

  protected override def cleanup(ctx: Mapper[VarLongWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    ctx.write(new VarIntWritable(RowSimilarityJobAnalytics.NORM_VECTOR_MARKER), new VectorWritable(norms))
    ctx.write(new VarIntWritable(RowSimilarityJobAnalytics.NUM_NON_ZERO_ENTRIES_VECTOR_MARKER), new VectorWritable(nonZeroEntries))
    ctx.write(new VarIntWritable(RowSimilarityJobAnalytics.MAXVALUE_VECTOR_MARKER), new VectorWritable(maxValues))
  }
}


class MergeVectorsReducer extends Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable] {
  var normsPath: Path = null
  var numNonZeroEntriesPath: Path = null
  var maxValuesPath: Path = null

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def setup(ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    normsPath = new Path(ctx.getConfiguration.get(RowSimilarityJobAnalytics.NORMS_PATH))
    numNonZeroEntriesPath = new Path(ctx.getConfiguration.get(RowSimilarityJobAnalytics.NUM_NON_ZERO_ENTRIES_PATH))
    maxValuesPath = new Path(ctx.getConfiguration.get(RowSimilarityJobAnalytics.MAXVALUES_PATH))
  }

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  protected override def reduce(row: VarIntWritable, partialVectors: java.lang.Iterable[VectorWritable], ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
    val partialVector: Vector = Vectors.merge(partialVectors)
    if (row.get == RowSimilarityJobAnalytics.NORM_VECTOR_MARKER) {
      Vectors.write(partialVector, normsPath, ctx.getConfiguration)
    }
    else if (row.get == RowSimilarityJobAnalytics.MAXVALUE_VECTOR_MARKER) {
      Vectors.write(partialVector, maxValuesPath, ctx.getConfiguration)
    }
    else if (row.get == RowSimilarityJobAnalytics.NUM_NON_ZERO_ENTRIES_VECTOR_MARKER) {
      Vectors.write(partialVector, numNonZeroEntriesPath, ctx.getConfiguration, true)
    }
    else {
      ctx.write(row, new VectorWritable(partialVector))
    }
  }
}


object RowSimilarityJobAnalytics {
  val NO_THRESHOLD: Double = Double.MinValue
  val NO_FIXED_RANDOM_SEED: Long = Long.MinValue
  val SIMILARITY_CLASSNAME: String = RowSimilarityJobAnalytics.getClass + ".distributedSimilarityClassname"
  val NUMBER_OF_COLUMNS: String = RowSimilarityJobAnalytics.getClass + ".numberOfColumns"
  val MAX_SIMILARITIES_PER_ROW: String = RowSimilarityJobAnalytics.getClass + ".maxSimilaritiesPerRow"
  val EXCLUDE_SELF_SIMILARITY: String = RowSimilarityJobAnalytics.getClass + ".excludeSelfSimilarity"
  val THRESHOLD: String = RowSimilarityJobAnalytics.getClass + ".threshold"
  val NORMS_PATH: String = RowSimilarityJobAnalytics.getClass + ".normsPath"
  val MAXVALUES_PATH: String = RowSimilarityJobAnalytics.getClass + ".maxWeightsPath"
  val NUM_NON_ZERO_ENTRIES_PATH: String = RowSimilarityJobAnalytics.getClass + ".nonZeroEntriesPath"
  val DEFAULT_MAX_SIMILARITIES_PER_ROW: Int = 100
  val OBSERVATIONS_PER_COLUMN_PATH: String = RowSimilarityJobAnalytics.getClass + ".observationsPerColumnPath"
  val MAX_OBSERVATIONS_PER_ROW: String = RowSimilarityJobAnalytics.getClass + ".maxObservationsPerRow"
  val MAX_OBSERVATIONS_PER_COLUMN: String = RowSimilarityJobAnalytics.getClass + ".maxObservationsPerColumn"
  val RANDOM_SEED: String = RowSimilarityJobAnalytics.getClass + ".randomSeed"
  val DEFAULT_MAX_OBSERVATIONS_PER_ROW: Int = 500
  val DEFAULT_MAX_OBSERVATIONS_PER_COLUMN: Int = 500
  val NORM_VECTOR_MARKER: Int = Integer.MIN_VALUE
  val MAXVALUE_VECTOR_MARKER: Int = Integer.MIN_VALUE + 1
  val NUM_NON_ZERO_ENTRIES_VECTOR_MARKER: Int = Integer.MIN_VALUE + 2


//  object CalculateSimilarityMatrix {
    def runJob(inputPath:String,outPutPath:String, inputFormatClass:Class[_<:FileInputFormat[_,_]],
               outputFormatClass:Class[_<:FileOutputFormat[_,_]], deleteFolder : Boolean,
               numMapTasks : Option[Int] = None, similarityClassnameArg:String): Unit ={

      //      addInputOption
      //      addOutputOption
      //      addOption("numberOfColumns", "r", "Number of columns in the input matrix", false)
      //      addOption("similarityClassname", "s", "Name of distributed similarity class to instantiate, alternatively use " + "one of the predefined similarities (" + VectorSimilarityMeasures.list + ')')
      //      addOption("maxSimilaritiesPerRow", "m", "Number of maximum similarities per row (default: " + DEFAULT_MAX_SIMILARITIES_PER_ROW + ')', String.valueOf(DEFAULT_MAX_SIMILARITIES_PER_ROW))
      //      addOption("excludeSelfSimilarity", "ess", "compute similarity of rows to themselves?", String.valueOf(false))
      //      addOption("threshold", "tr", "discard row pairs with a similarity value below this", false)
//            addOption("maxObservationsPerRow", null, "sample rows down to this number of entries", String.valueOf(DEFAULT_MAX_OBSERVATIONS_PER_ROW))
//            addOption("maxObservationsPerColumn", null, "sample columns down to this number of entries", String.valueOf(DEFAULT_MAX_OBSERVATIONS_PER_COLUMN))
//            addOption("randomSeed", null, "use this seed for sampling", false)
      //      addOption(DefaultOptionCreator.overwriteOption.create)

      //      val parsedArgs: java.util.Map[java.lang.String, java.util.List[java.lang.String]] =  parseArguments(args)

      //      if (parsedArgs == null) {
      //        return -1
      //      }
//      var numberOfColumns: Int = 0
//      if (hasOption("numberOfColumns")) {
//        numberOfColumns = getOption("numberOfColumns").toInt
//      }
     // else {
//        numberOfColumns = getDimensions(getInputPath)
      //}
      //      val similarityClassnameArg: String = getOption("similarityClassname")
      var similarityClassname: String = null
      try {
        similarityClassname = VectorSimilarityMeasures.valueOf(similarityClassnameArg).getClassname
      }
      catch {
        case iae: IllegalArgumentException => {
          similarityClassname = similarityClassnameArg
        }
      }

      //      if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
      //        HadoopUtil.delete(getConf, getTempPath)
      //        HadoopUtil.delete(getConf, getOutputPath)
      //      }

      //      val maxSimilaritiesPerRow: Int = getOption("maxSimilaritiesPerRow").toInt
      //      val excludeSelfSimilarity: Boolean = getOption("excludeSelfSimilarity").toBoolean
     // val threshold: Double = if (hasOption("threshold")) getOption("threshold").toDouble else NO_THRESHOLD
       val threshold: Double =NO_THRESHOLD

     // val randomSeed: Long = if (hasOption("randomSeed")) getOption("randomSeed").toLong else NO_FIXED_RANDOM_SEED
     val randomSeed: Long = NO_FIXED_RANDOM_SEED

//      val maxObservationsPerRow: Int = getOption("maxObservationsPerRow").toInt
//      val maxObservationsPerColumn: Int = getOption("maxObservationsPerColumn").toInt


      val maxObservationsPerRow: Int = DEFAULT_MAX_OBSERVATIONS_PER_ROW
      val maxObservationsPerColumn: Int = DEFAULT_MAX_OBSERVATIONS_PER_COLUMN

      val weightsPath: Path = new Path("weight");
      val normsPath: Path = new Path("norms.bin")
      val numNonZeroEntriesPath: Path = new Path("numNonZeroEntries.bin")
      val maxValuesPath: Path = new Path("maxValues.bin")
            val pairwiseSimilarityPath: Path = new Path("pairwiseSimilarity")
      val observationsPerColumnPath: Path = new Path("observationsPerColumn.bin")
            val currentPhase: AtomicInteger = new AtomicInteger
            val countObservations: Job = prepareJob(getInputPath, getTempPath("notUsed"), classOf[RowSimilarityJob.CountObservationsMapper], classOf[NullWritable], classOf[VectorWritable], classOf[RowSimilarityJob.SumObservationsReducer], classOf[NullWritable], classOf[VectorWritable])
            countObservations.setCombinerClass(classOf[VectorSumCombiner])
            countObservations.getConfiguration.set(OBSERVATIONS_PER_COLUMN_PATH, observationsPerColumnPath.toString)
            countObservations.setNumReduceTasks(1)
            countObservations.waitForCompletion(true)



      //TODO falta COMBINER
      val normsAndTranspose: Job = MapReduceUtils.prepareJob("Similarity",
        classOf[VectorNormMapper],
        classOf[MergeVectorsReducer],
        classOf[VarIntWritable],
        classOf[VectorWritable],
        classOf[VarIntWritable],
        classOf[VectorWritable],
        classOf[SequenceFileInputFormat[VarIntWritable,VectorWritable]],
        classOf[TextOutputFormat[VarIntWritable,VectorWritable]],
      inputPath,
      outPutPath)


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
      val succeeded: Boolean = normsAndTranspose.waitForCompletion(true)

    }
//  }



  @throws(classOf[Exception])
  def run(args: Array[String]): Int= {
    return 0;
  }

  class MergeVectorsCombiner extends Reducer[VarIntWritable,VectorWritable,VarIntWritable,VectorWritable] {
    @throws(classOf[IOException])
    @throws(classOf[InterruptedException])                                                                  /*[KEYIN, VALUEIN, KEYOUT, VALUEOUT]*/
   protected override def reduce(row: VarIntWritable, partialVectors: java.lang.Iterable[VectorWritable], ctx: Reducer[VarIntWritable,VectorWritable,VarIntWritable,VectorWritable]#Context) = {
      ctx.write(row, new VectorWritable(Vectors.merge(partialVectors)))
    }
  }

  object Counters extends Enumeration {
    type Counters = Value
    val ROWS, USED_OBSERVATIONS, NEGLECTED_OBSERVATIONS, COOCCURRENCES, PRUNED_COOCCURRENCES = Value
  }

  protected def shouldRunNextPhase(args: java.util.Map[String, java.util.List[String]], currentPhase: AtomicInteger): Boolean = {
    var phase = currentPhase.getAndIncrement();
    val startPhase = AbstractJob.getOption(args, "--startPhase");
    var endPhase = AbstractJob.getOption(args, "--endPhase");
    var phaseSkipped = startPhase != null && phase < Integer.parseInt(startPhase) || endPhase != null && phase > Integer.parseInt(endPhase);
    if (phaseSkipped) {
      /*TODO
      log.info("Skipping phase {}", Integer.valueOf(phase));
      */
    }

    return !phaseSkipped;
  }








  object CooccurrencesMapper {
    val BY_INDEX: Comparator[Vector.Element] = new Comparator[Element] {
      override def compare(one: Element, two: Element): Int = {
        return Ints.compare(one.index, two.index)
      }
    }

    class CooccurrencesMapper extends Mapper[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable] {
      var similarity: VectorSimilarityMeasure = null
      var numNonZeroEntries: OpenIntIntHashMap = null
      var maxValues: Vector = null
      var threshold: Double = .0

      @throws(classOf[IOException])
      @throws(classOf[InterruptedException])
      protected override def setup(ctx: Mapper[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
        similarity = ClassUtils.instantiateAs(ctx.getConfiguration.get(SIMILARITY_CLASSNAME), classOf[VectorSimilarityMeasure])
        numNonZeroEntries = Vectors.readAsIntMap(new Path(ctx.getConfiguration.get(NUM_NON_ZERO_ENTRIES_PATH)), ctx.getConfiguration)
        maxValues = Vectors.read(new Path(ctx.getConfiguration.get(MAXVALUES_PATH)), ctx.getConfiguration)
        threshold = ctx.getConfiguration.get(THRESHOLD).toDouble
      }

      private def consider(occurrenceA: Vector.Element, occurrenceB: Vector.Element): Boolean = {
        val numNonZeroEntriesA: Int = numNonZeroEntries.get(occurrenceA.index)
        val numNonZeroEntriesB: Int = numNonZeroEntries.get(occurrenceB.index)
        val maxValueA: Double = maxValues.get(occurrenceA.index)
        val maxValueB: Double = maxValues.get(occurrenceB.index)
        return similarity.consider(numNonZeroEntriesA, numNonZeroEntriesB, maxValueA, maxValueB, threshold)
      }

      @throws(classOf[IOException])
      @throws(classOf[InterruptedException])
      protected override def map(column: VarIntWritable, occurrenceVector: VectorWritable, ctx: Mapper[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
        val occurrences: Array[Vector.Element] = Vectors.toArray(occurrenceVector)
        Arrays.sort(occurrences, CooccurrencesMapper.BY_INDEX)
        var cooccurrences: Int = 0
        var prunedCooccurrences: Int = 0
        for (n: Int <- 0 to occurrences.length) {
          print(n)
          val occurrenceA: Vector.Element = occurrences(n);
          val dots: Vector = new RandomAccessSparseVector(Integer.MAX_VALUE);
          for (m: Int <- n to occurrences.length) {
            val occurrenceB:Vector.Element = occurrences(m);
            if (threshold == NO_THRESHOLD || consider(occurrenceA, occurrenceB)) {
              dots.setQuick(occurrenceB.index(), similarity.aggregate(occurrenceA.get(), occurrenceB.get()));
              cooccurrences = cooccurrences+1;
            } else {
              prunedCooccurrences = prunedCooccurrences+1;
            }
          }
          ctx.write(new VarIntWritable(occurrenceA.index()), new VectorWritable(dots));
        }
        /*ctx.getCounter(Counters.COOCCURRENCES).increment(cooccurrences)
        ctx.getCounter(Counters.PRUNED_COOCCURRENCES).increment(prunedCooccurrences)*/
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
      similarity = ClassUtils.instantiateAs(ctx.getConfiguration.get(SIMILARITY_CLASSNAME), classOf[VectorSimilarityMeasure])
      numberOfColumns = ctx.getConfiguration.getInt(NUMBER_OF_COLUMNS, -1)
      //Preconditions.checkArgument(numberOfColumns > 0, "Number of columns must be greater then 0! But numberOfColumns = " + numberOfColumns)
      excludeSelfSimilarity = ctx.getConfiguration.getBoolean(EXCLUDE_SELF_SIMILARITY, false)
      norms = Vectors.read(new Path(ctx.getConfiguration.get(NORMS_PATH)), ctx.getConfiguration)
      treshold = ctx.getConfiguration.get(THRESHOLD).toDouble
    }

    @throws(classOf[IOException])
    @throws(classOf[InterruptedException])
    override def reduce(row: VarIntWritable, partialDots: java.lang.Iterable[VectorWritable], ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
      val partialDotsIterator: Iterator[VectorWritable] = partialDots.iterator
      val dots: Vector = partialDotsIterator.next.get
      while (partialDotsIterator.hasNext) {
        val toAdd: Vector = partialDotsIterator.next.get
        for (nonZeroElement <- toAdd.nonZeroes) {
          dots.setQuick(nonZeroElement.index, dots.getQuick(nonZeroElement.index) + nonZeroElement.get)
        }
      }
      val similarities: Vector = dots.like
      val normA: Double = norms.getQuick(row.get)
      for (b <- dots.nonZeroes) {
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
      maxSimilaritiesPerRow = ctx.getConfiguration.getInt(MAX_SIMILARITIES_PER_ROW, 0)
      //Preconditions.checkArgument(maxSimilaritiesPerRow > 0, "Maximum number of similarities per row must be greater then 0!")
    }

    @throws(classOf[IOException])
    @throws(classOf[InterruptedException])
    protected override def map(row: VarIntWritable, similaritiesWritable: VectorWritable, ctx: Mapper[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
      val similarities: Vector = similaritiesWritable.get
      val transposedPartial: Vector = new RandomAccessSparseVector(similarities.size, 1)
      val topKQueue: TopElementsQueue = new TopElementsQueue(maxSimilaritiesPerRow)
      for (nonZeroElement <- similarities.nonZeroes) {
        val top: MutableElement = topKQueue.top
        val candidateValue: Double = nonZeroElement.get
        if (candidateValue > top.get) {
          top.setIndex(nonZeroElement.index)
          top.set(candidateValue)
          topKQueue.updateTop
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
      maxSimilaritiesPerRow = ctx.getConfiguration.getInt(MAX_SIMILARITIES_PER_ROW, 0)
     // Preconditions.checkArgument(maxSimilaritiesPerRow > 0, "Maximum number of similarities per row must be greater then 0!")
    }

    @throws(classOf[IOException])
    @throws(classOf[InterruptedException])
    protected override def reduce(row: VarIntWritable, partials:  java.lang.Iterable[VectorWritable], ctx: Reducer[VarIntWritable, VectorWritable, VarIntWritable, VectorWritable]#Context) {
      val allSimilarities: Vector = Vectors.merge(partials)
      val topKSimilarities: Vector = Vectors.topKElements(maxSimilaritiesPerRow, allSimilarities)
      ctx.write(row, new VectorWritable(topKSimilarities))
    }
  }

}
