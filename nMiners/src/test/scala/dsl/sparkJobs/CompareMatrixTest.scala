package dsl.sparkJobs

import org.apache.mahout.math.Matrix
import org.scalactic.Equality

import scala.collection.JavaConversions.asScalaIterator

trait CompareMatrixTest {

  /**
   * Object to compare if two matrixes are equals
   */
  implicit val matrixEquality = new Equality[Matrix] {
    override def areEqual(a: Matrix, b: Any): Boolean = {

      if (!b.isInstanceOf[Matrix]) {
        false
      } else {
        val m = b.asInstanceOf[Matrix]
        if (a.numCols() != m.numCols() || a.numRows() != m.numRows()) {
          false
        } else {
          var res = true
          for (i <- 0 until m.numRows()) {
            a.viewRow(i).all().iterator().zip {
              m.viewRow(i).all().iterator()
            }.foreach { t => t match {
              case (e1, e2) => {
                res &= e1.get == e2.get
              }
            }
            }
          }
          res
        }
      }
    }
  }

}
