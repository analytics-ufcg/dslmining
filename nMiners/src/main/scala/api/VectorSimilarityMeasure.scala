package api

import org.apache.mahout.math.Vector

/**
 * Created by arthur on 29/04/15.
 */
trait VectorSimilarityMeasure {
  val NO_NORM: Double = 0.0
  def normalize(vector: Vector): Vector
  def norm(vector: Vector): Double
  def aggregate(v: Double, v1: Double): Double
  def similarity(v: Double, v1: Double, v2: Double, i: Int): Double
  def consider(i: Int, i1: Int, v: Double, v1: Double, v2: Double): Boolean
}
