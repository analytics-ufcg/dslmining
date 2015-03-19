package dsl

import api.UserBasedRecommenderImpl
import dsl.Implicits.WithPath

/**
 * Created by andryw on 17/03/15.
 */
object Implicits {

  type WithPath = String
  type NeighbourHoodSize = Int
  type WithRecommendationType = (WithPath,RecommendationType)
  type withSimilarity = (WithPath,RecommendationType,SimilarityType)
  type withNeighbourhood = (WithPath,RecommendationType,SimilarityType,NeighbourHoodSize)

  type UserId = Long
  type NumberOfItens = Int
  type WithNumberOfItems = (UserId,NumberOfItens)


  implicit def WithNumberOfItems(userId: UserId): UserIdHelper =
    new UserIdHelper(userId)


  implicit def WithPath2RecommenderHelper(path: WithPath): RecommenderHelper =
      new RecommenderHelper(path)

  implicit def PathRecom2SimilHelper(pr: WithRecommendationType): SimilarityHelper =
    new SimilarityHelper(pr)

  implicit def WithNeighboorhood(prs: withSimilarity): NeighbourhoodHelper =
    new NeighbourhoodHelper(prs)

  implicit def Tuple2Recommender(t: (WithPath, RecommendationType, SimilarityType, NeighbourHoodSize)):UserBasedRecommenderImpl = {
    t match {
      case (path,recommendationType,similarityType,neighbourHoodSize) => {
        UserBasedRecommenderImpl(path,recommendationType,similarityType,neighbourHoodSize)
      }
    }
  }

  class UserIdHelper(userId:UserId){
    def numberOfItens(numberOfItens:NumberOfItens): WithNumberOfItems ={
      new WithNumberOfItems (userId,numberOfItens)
    }
  }

  class RecommenderHelper(path: WithPath){
    def a(recommenderType:RecommendationType): WithRecommendationType ={
      new WithRecommendationType (path,recommenderType)
    }
  }

  class SimilarityHelper(tuple: WithRecommendationType){
    def using(similarity:SimilarityType): withSimilarity = {
      new withSimilarity(tuple._1,tuple._2,similarity)
    }
  }

  class NeighbourhoodHelper(tuple: withSimilarity){
    def neighbourhoodSize(neighbourHoodSize:NeighbourHoodSize): withNeighbourhood = {
      new withNeighbourhood(tuple._1,tuple._2,tuple._3,neighbourHoodSize)
    }
  }
}

