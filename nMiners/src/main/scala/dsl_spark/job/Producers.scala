package dsl_spark.job

//import api_hadoop.RecommenderJob

import api_spark.SimilarityMatrix
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

/**
 * Producer is a class that produce results. These results (produceds) can be used by any next command, not only the immediately next command
 */
abstract class Producer extends Job {
  var produced: Produced

  /**
   * If an outputPath isn't set, a default path is attributed: data_produced.name
   */
  def generateOutputPath(): Unit = {
    this.pathToOutput match {
      case None => this.pathToOutput = defaltOutputPath(Context.basePath)
      case _ =>
    }
  }

  def defaltOutputPath(basePath: String): Option[String] = {
    Some(basePath + "/data_" + produced.name)
  }

  override def run() = {
    super.run()
    //Before run, generate the correct output path
    this.generateOutputPath()

    //Save the output path to the Context, so another job could read the result
    Context.paths(produced.name) = pathToOutput.getOrElse("") + "/part-r-00000"
  }
}


/**
 * Is a object that can produce user vectors
 */
object user_vectors extends Producer {
  override var produced: Produced = _
  override var name: String = this.getClass.getSimpleName
  override val logger = LoggerFactory.getLogger(this.getClass())
  this.pathToOutput = Some(new Path(System.getProperty("java.io.tmpdir"), "userVectors").toString)
//  this.pathToOutput = Some(pathToInput)


  // Run the job
  override def run() = {
    super.run()

    //The input is the initial input
    //    pathToInput = Context.getInputPath()

    val arguments = s"--input $pathToInput --output $pathToOutput --booleanData true -s SIMILARITY_COSINE --outputType binary" split " "
//    new RecommenderJob uservector arguments
  }
}


/**
 * Is a object that can produce similarity matrix
 */
object similarity_matrix extends Producer {
  override var produced: Produced = _
  override val logger = LoggerFactory.getLogger(this.getClass())
  override var name: String = this.getClass.getSimpleName
  var similarity: SimilarityType = null;

  this.pathToOutput = Some(new Path(System.getProperty("java.io.tmpdir"), "similarityMatrix").toString)

  def using(similarity: SimilarityType): Producer = {
    this.similarity = similarity
    this
  }


  override def run() = {
    super.run()
    this pathToInput = Context.getInputPath()
    SimilarityMatrix.run(pathToInput, pathToOutput,"local")

//    val arguments = s"--input $pathToInput --output $pathToOutput --booleanData true -s SIMILARITY_COSINE" split " "
//    new RecommenderJob rowSimilarity(arguments, 10)


    //    val BASE_PATH = pathToOutput.get
    //    pathToOutput = Some(pathToOutput.get + "/matrix")
    //    Context.paths(produced.name) = pathToOutput.getOrElse("") + "/part-r-00000"
    //
    //
    //    SimilarityMatrix.generateSimilarityMatrix(pathToInput + "/part-r-00000",
    //      pathToOutput.get,
    //      classOf[SequenceFileInputFormat[VarLongWritable, VectorWritable]],
    //      classOf[SequenceFileOutputFormat[IntWritable, VectorWritable]], true, similarityClassnameArg = this.similarity._type, basePath = BASE_PATH, numReduceTasks = numProcess)
  }

}

//Copy the prediction matrix to the output specified by the user
object recommendation extends Producer {
  override var name: String = this.getClass.getSimpleName
  override var produced: Produced = _
  override val logger = LoggerFactory.getLogger(this.getClass())

  override def run() = {
    super.run

    val out = pathToOutput.get
    val arguments = s"--input $pathToInput --output $out --booleanData true -s SIMILARITY_COSINE" split " "
//    new RecommenderJob recommender arguments
//    val outputFile = new File(pathToOutput.get)
//
//    val jobConf = new JobConf(new Configuration())
//    val fs1: FileSystem = FileSystem.get(jobConf)
//    fs1.mkdirs(new Path(outputFile.getParent))
//
//    FileUtil.copy(fs1, new Path(pathToInput + "/part-r-00000"), fs1, new Path(pathToOutput.get), false, new Configuration())
  }

}