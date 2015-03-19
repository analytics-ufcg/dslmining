package dsl

abstract class SimilarityType (_type:String)

case object PEARSON_CORRELATION extends SimilarityType("PEARSON")
case object EUCLIDEAN_DISTANCE extends SimilarityType("EUCLIDEAN")
