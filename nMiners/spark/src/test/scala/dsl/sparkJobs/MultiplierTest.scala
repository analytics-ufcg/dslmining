package dsl.sparkJobs

import dsl.job._
import org.apache.mahout.math.drm.{DrmLike, drmParallelize}
import org.apache.mahout.math.scalabindings._
import org.apache.mahout.sparkbindings._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import dsl.Job.CompareMatrixTest

class MultiplierTest extends FlatSpec with Matchers with BeforeAndAfterAll with CompareMatrixTest {

  var matrixA: DrmLike[Int] = _
  var matrixB: DrmLike[Int] = _
  var matrixAxB: DrmLike[Int] = _

  var producedA: Produced[DrmLike[Int]] = _
  var producedB: Produced[DrmLike[Int]] = _

  implicit val context = mahoutSparkContext(masterUrl = "local",
    appName = "MahoutLocalContext")

  override def beforeAll = {
    matrixA = drmParallelize(dense(
      (1, 0),
      (1, 1)
    ))
    matrixB = drmParallelize(dense(
      (2, 2),
      (5, 4)
    ))
    matrixAxB = drmParallelize(dense(
      (2, 2),
      (7, 6)
    ))
    producedA = new Produced[DrmLike[Int]]("producedA", null)
    producedA.product = matrixA

    producedB = new Produced[DrmLike[Int]]("producedA", null)
    producedB.product = matrixB
  }

  override def afterAll = {
    Context.clearQueues
  }

  "MultiplierJob" should "produce a produced named \'Multiplier produced1_name by produced2_name\'" in {

    val mult = new Multiplier(producedA, producedB)
    mult.run

    Context.produceds should contain(Produced(name = mult.name))
  }

  it should "multiply right" in {

    val mult = new Multiplier(producedA, producedB)
    mult.run

    val producedResultName = mult.getClass.getSimpleName + s" $producedA by $producedB"

    val resutantMatrix = Context producedsByName producedResultName map {
      _.product.asInstanceOf[DrmLike[Int]]
    } getOrElse {
      fail(s"Multiplier Job should produce a produced named $producedResultName")
    }

    resutantMatrix.collect should equal(matrixAxB.collect)
  }
}
