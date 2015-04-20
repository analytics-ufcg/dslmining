package dsl.job

/**
 * Created by viana on 17/04/15.
 */
abstract class SimilarityType (_type:String)

case object PEARSON_CORRELATION extends SimilarityType("PEARSON")
case object EUCLIDEAN_DISTANCE extends SimilarityType("EUCLIDEAN")
