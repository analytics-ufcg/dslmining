package dsl

/**
 * Created by andryw on 17/03/15.
 */
abstract class SimilarityType (_type:String)

case object PEARSON_CORRELATION extends SimilarityType("PEARSON")
case object EUCLIDEAN_DISTANCE extends SimilarityType("EUCLIDEAN")
