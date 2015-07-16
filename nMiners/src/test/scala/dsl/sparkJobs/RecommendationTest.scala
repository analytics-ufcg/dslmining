package dsl.sparkJobs

import dsl_spark.job.{Context, Multiplier, Produced, _}
import org.apache.mahout.math.drm.{DrmLike, drmParallelize, _}
import org.apache.mahout.math.scalabindings._
import org.apache.mahout.sparkbindings._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class RecommendationTest extends FlatSpec with Matchers with BeforeAndAfterAll with CompareMatrixTest {

  var userVector: DrmLike[Int] = _
  var recommendationMatrix: DrmLike[Int] = _
  var resultingMatrix: DrmLike[Int] = _

  var producedUserVector: Produced[DrmLike[Int]] = _
  var producedRecommendationMatrix: Produced[DrmLike[Int]] = _

  implicit val context = mahoutSparkContext(masterUrl = "local",
    appName = "MahoutLocalContext")

  override def beforeAll = {
  
    userVector = drmParallelize(dense(
      (0, 0, 0),
      (1, 0, 0),
      (0, 1, 0),
      (0, 0, 1),
      (1, 1, 0),
      (0, 1, 1),
      (1, 1, 1)
    ))
    recommendationMatrix = drmParallelize(dense(
      (0, 0.56, 0.12),
      (0.88, 0.5, 0),
      (0, 0.44, 0.33),
      (0.65, 0.23, 0.87),
      (0.88, 0.32, 0),
      (0, 0.52, 0.99),
      (0.52, 0.65, 0.37)
    ))
    resultingMatrix = drmParallelize(dense(
      (0, 0.56, 0.12),
      (0, 0.5, 0),
      (0, 0, 0.33),
      (0.65, 0.23, 0),
      (0, 0, 0),
      (0, 0, 0),
      (0, 0, 0)
    ))

    producedUserVector = new Produced[DrmLike[Int]]("userVector", user_vectors)
    producedUserVector.product = userVector
    producedUserVector.producer.produced = producedUserVector

    producedRecommendationMatrix = new Produced[DrmLike[Int]]("Multiplier", new Multiplier(null, null))
    producedRecommendationMatrix.product = recommendationMatrix
    producedRecommendationMatrix.producer.produced = producedRecommendationMatrix

    Context.produceds ++= List(producedUserVector, producedRecommendationMatrix)

    recommendation.run
  }

  "recommender job" should "produce a produced named recommendation" in {
    Context.produceds should contain(Produced(name = recommendation.name))
  }

  it should "not recommend to a user a item he already consumed" in {
    val resultantMatrix = Context producedsByName recommendation.name map {
      _.product.asInstanceOf[DrmLike[Int]]
    } getOrElse {
      fail("Recommender Job should produce a produced named recommendation")
    }

    resultantMatrix.collect should equal(resultingMatrix.collect)
  }
}