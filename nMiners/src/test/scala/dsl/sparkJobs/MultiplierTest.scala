package dsl.sparkJobs

import dsl_spark.job.{Context, Multiplier, Produced}
import org.apache.mahout.math.Matrix
import org.apache.mahout.math.drm.{DrmLike, drmParallelize}
import org.apache.mahout.math.scalabindings._
import org.apache.mahout.sparkbindings._
import org.scalactic.Equality
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Created by igleson on 06/07/15.
 */
class MultiplierTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  var matrixA: DrmLike[Int] = _
  var matrixB: DrmLike[Int] = _
  var matrixAxB: DrmLike[Int] = _

  var producedA: Produced[DrmLike[Int]] = _
  var producedB: Produced[DrmLike[Int]] = _

  implicit val context = mahoutSparkContext(masterUrl = "local",
    appName = "MahoutLocalContext")

  implicit val matrixEquality = new Equality[Matrix] {
    override def areEqual(a: Matrix, b: Any): Boolean = {
      if (! b.isInstanceOf[Matrix]) {
        false
      } else {
        val m = b.asInstanceOf[Matrix]
        a.
      }
    }
  }

  override def beforeAll(): Unit = {

    matrixA = drmParallelize(dense(
      (1, 2),
      (3, 4)
    ))
    matrixB = drmParallelize(dense(
      (2, 2),
      (5, 4)
    ))
    matrixAxB = drmParallelize(dense(
      (12, 10),
      (26, 26)
    ))
    producedA = new Produced[DrmLike[Int]]("producedA", null)
    producedA.product = matrixA

    producedB = new Produced[DrmLike[Int]]("producedA", null)
    producedB.product = matrixB
  }

  "MultiplierJob" should "multiply right" in {

    val mult = new Multiplier(producedA, producedB)
    mult.run

    val producedResultName = mult.getClass.getSimpleName + s" $producedA by $producedB"

    val resutantMatrix = Context.produceds.find(_.name equals producedResultName).map {
      _.product.asInstanceOf[DrmLike[Int]]
    }  getOrElse {
      fail(s"Multiplier Job should produce a produced named $producedResultName")
    }

    println( matrixAxB.collect equals resutantMatrix.collect )

    resutantMatrix.collect should equal(matrixAxB.collect)
  }
}
