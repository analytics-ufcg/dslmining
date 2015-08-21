package spark.dsl.job

import spark.api.{ItemSimilarityDriver, UserVectorDriver}

import spark.api.UserVectorDriver
import org.apache.mahout.math.drm
import org.apache.mahout.math.drm.DrmLike
import org.apache.mahout.math.drm.RLikeDrmOps._
import org.slf4j.LoggerFactory
import utils.Writer

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
      Writer.writeDRM_userItem(this.produced.product, this.pathToOutput.get)

    }
  }
  // Run the job
  override def run() = {
//    this.produced = new Produced(this.name,this)
    super.run()
    //REMOVE FROM HERE
    UserVectorDriver.start()

    val userVectorDrm = UserVectorDriver.run(Array(
      "--input", pathToInput,
      "--output", pathToOutput.getOrElse(""),
      "--master", "local"
    ))


    this.produced.product = userVectorDrm(0)
    this.produced.producer = this

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
      "--output", this.pathToOutput.getOrElse(""),
      "--master", "local"
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
      Writer.writeDRM_userItem(this.produced.product, this.pathToOutput.get)
    }
  }

  override def run() = {
    super.run

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