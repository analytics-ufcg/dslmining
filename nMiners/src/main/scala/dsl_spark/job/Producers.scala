package dsl_spark.job

import api_spark.UserVectorDriver
import org.apache.mahout.math.drm.DrmLike
import org.apache.mahout.math.drm.RLikeDrmOps._
import org.slf4j.LoggerFactory

/**
 * Producer is a class that produce results. These results (produceds) can be used by any next command, not only the immediately next command
 */
trait Producer[A] extends Job {
  var produced: Produced[A] = _

  override def run() = {
    super.run()

  }
}


/**
 * Is a object that can produce user vectors
 */
object user_vectors extends Producer[DrmLike[Int]] {
  override var name = this.getClass.getSimpleName
  override val logger = LoggerFactory.getLogger(this.getClass())

  // Run the job
  override def run() = {
    this.produced = new Produced(this.name,this)

    super.run()

    //REMOVE FROM HERE
    UserVectorDriver.start()

    val userVectorDrm = UserVectorDriver.run(Array(
      "--input", pathToInput,
      "--output", pathToOutput.getOrElse(""),
      "--master", "local"
    ))

    this.produced.product = userVectorDrm(0)

  }
}


/**
 * Is a object that can produce similarity matrix
 */
object similarity_matrix extends Producer[DrmLike[Int]] {
  override val logger = LoggerFactory.getLogger(this.getClass())
  override var name = this.getClass.getSimpleName

  override def run() = {
    super.run()
  }

}

//Copy the prediction matrix to the output specified by the user
object recommendation extends Producer[DrmLike[Int]] {
  override var name = this.getClass.getSimpleName
  override val logger = LoggerFactory.getLogger(this.getClass())

  produced = new Produced[DrmLike[Int]](this.name, this)

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