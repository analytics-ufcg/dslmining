package spark.dsl.job

import java.io.FileInputStream
import java.util.Properties

import spark.api
import spark.api.{UserVectorDriver, ItemSimilarityDriver}
import org.apache.mahout.math.drm
import org.apache.mahout.math.drm._
import org.apache.mahout.math.drm.DrmLike
import org.apache.mahout.math.drm.RLikeDrmOps._
import org.slf4j.LoggerFactory
import utils.{Holder, Writer}
import org.apache.mahout.sparkbindings.drm.{DrmRddInput, CheckpointedDrmSpark}
import org.apache.mahout.math.drm.{DrmLike, DrmLikeOps, DistributedContext, CheckpointedDrm}
import org.apache.mahout.sparkbindings._
import org.apache.mahout.sparkbindings._
import org.apache.mahout.sparkbindings

/**
 * Producer is a class that produce results. These results (produceds) can be used by any next command, not only the immediately next command
 */
trait Producer[A] extends Job {
  var produced: Produced[A] = _

  def clear() = {
    this.isWiretable = false;
  }

  override def run() = {
    super.run()

  }
}


/**
 * Is a object that can produce user vectors
 */
object user_vectors extends Producer[drm.DrmLike[Int]] {
  override var name = this.getClass.getSimpleName
  override val logger = LoggerFactory.getLogger(this.getClass())
  override def afterJob(): Unit ={
    if (this.isWiretable) {
      Writer.writeDRM_userItem(this.produced.product.asInstanceOf[CheckpointedDrmSpark[Int]], this.pathToOutput.get)

    }
  }
  // Run the job
  override def run() = {
//    this.produced = new Produced(this.name,this)
    super.run()
    //REMOVE FROM HERE
    println("\nvai startar UserVectorDriver para o master "+Context.masterUrl)
    UserVectorDriver.start(Context.masterUrl, Context.jar)
    println("\nStartou")

    val userVectorDrm = UserVectorDriver.run(Array(
      "--input", pathToInput,
      "--output", pathToOutput.getOrElse("")
    ))

    var partitions = 4;
    try{
      val prop = new Properties()
      prop.load(new FileInputStream("config.prop"))
      partitions = new Integer(prop.getProperty("nMiners.partitions"))
    } catch {
        case e: Exception =>
          e.printStackTrace()
          sys.exit(-1)
    }
    //this.produced.product = userVectorDrm(0).asInstanceOf[CheckpointedDrmSpark[Int]]
    this.produced.product = drmParallelize(userVectorDrm(0), partitions) (UserVectorDriver.getContext())
    Context.partitions = partitions
    println("*PARTITIONS "+partitions)
    //val t = this.produced.product.rdd.repartition(partitions)
    //this.produced.product = drmWrap(t, this.produced.product.nrow, this.produced.product.ncol)

    this.produced.product.rdd.partitions.length

    this.produced.producer = this
    Holder.logger.info("USER VECTOR RDD ")

    Writer.context = UserVectorDriver.getContext()
    Writer.indexedDataset = UserVectorDriver.indexedDataset
    Writer.writeSchema = UserVectorDriver.writeSchema

    //    Context.produceds.add(this.produced)
  }


}


/**
 * Is a object that can produce similarity matrix
 */
object similarity_matrix extends Producer[DrmLike[Int]] {
  override val logger = LoggerFactory.getLogger(this.getClass())
  override var name = this.getClass.getSimpleName
//  this.produced = new Produced(this.name,this)
  override def afterJob(): Unit ={
    if (this.isWiretable) {
      Writer.writeDRM_itemItem(this.produced.product, this.pathToOutput.get)
    }
  }

  override def run() = {
    super.run()

    val itemSimilarity = ItemSimilarityDriver.run(Array(user_vectors.produced.product), Array(
      "--input", pathToInput,
      "--output", this.pathToOutput.getOrElse("")
    ))(UserVectorDriver.getParser(),UserVectorDriver.getContext(), UserVectorDriver.indexedDataset)

    this.produced.product = itemSimilarity(0)
    this.produced.producer = this

    //    Context.produceds.add(this.produced)

  }

}

//Copy the prediction matrix to the output specified by the user
object recommendation extends Producer[DrmLike[Int]] {
  override var name = this.getClass.getSimpleName
  override val logger = LoggerFactory.getLogger(this.getClass())

  produced = new Produced[DrmLike[Int]](this.name, this)


  override def afterJob(): Unit ={
    if (this.isWiretable) {
      //this.produced.product = drmParallelize(this.produced.product, 16) (UserVectorDriver.getContext())
      Writer.writeDRM_userItem(this.produced.product, this.pathToOutput.get)
    }
  }

  override def run() = {
    super.run

    print("Rodando recommendation")

    val recMatrixProduced = Context.producedsByType[Multiplier].getOrElse {
      throw new IllegalStateException("You must multiply the user vector by the similarity matrix first")
    }

    val userVectorProduced = Context.producedsByType[user_vectors.type].getOrElse {
      throw new IllegalStateException("You must produce a user vector first")
    }

    val userVector = userVectorProduced.produced.product
    val recMatrix = recMatrixProduced.produced.product

    //invert the user vector matrix
    produced.product = recMatrix * ((userVector - 1) * -1)
    Context.produceds += produced
  }

}