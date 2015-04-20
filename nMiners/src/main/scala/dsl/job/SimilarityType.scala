package dsl.job

/**
 * Abstract class that is responsible to create all Similarity Type
 * @param _type Similarity Type
 */
abstract class SimilarityType (_type:String)

case object PEARSON_CORRELATION extends SimilarityType("PEARSON")
case object EUCLIDEAN_DISTANCE extends SimilarityType("EUCLIDEAN")
