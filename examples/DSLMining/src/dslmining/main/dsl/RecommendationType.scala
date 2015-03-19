package dsl

/**
 * Created by andryw on 17/03/15.
 */
abstract class RecommendationType (_type:String)

case object USER_BASED_RECOMMENDER extends RecommendationType("USERBASED")
case object MATRIX_FAC_RECOMMENDER extends RecommendationType("MATRIXFACTORIZATION")
