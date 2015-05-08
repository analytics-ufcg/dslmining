package dsl.job

/**
 * Abstract class that is responsible to create all Similarity Type
 * @param _type Similarity Type
 */
abstract class SimilarityType (val _type:String)

case object PEARSON_CORRELATION extends SimilarityType("SIMILARITY_PEARSON_CORRELATION")
case object EUCLIDEAN_DISTANCE extends SimilarityType("SIMILARITY_EUCLIDEAN_DISTANCE")
case object COOCURRENCE extends SimilarityType("SIMILARITY_COOCCURRENCE")
case object COSINE extends SimilarityType("SIMILARITY_COSINE")
case object CITY_BLOCK extends SimilarityType("SIMILARITY_CITY_BLOCK")
case object LOGLIKELIHOOD extends SimilarityType("SIMILARITY_LOGLIKELIHOOD")
case object JACCARD extends SimilarityType("SIMILARITY_TANIMOTO_COEFFICIENT")
