package api

import java.io.File
import scala.collection.JavaConversions._
import dsl.Implicits.{WithNumberOfItems, UserId, NeighbourHoodSize, WithPath}
import dsl.{RecommendationType, EUCLIDEAN_DISTANCE, PEARSON_CORRELATION, SimilarityType}
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity

import org.apache.mahout.cf.taste.model.DataModel
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood
import org.apache.mahout.cf.taste.recommender.{RecommendedItem, Recommender}
import org.apache.mahout.cf.taste.similarity.UserSimilarity

/**
 * Created by andryw on 17/03/15.
 */
case class UserBasedRecommenderImpl (val path: WithPath, val recomType: RecommendationType, val similarityType: SimilarityType, val neighbourHoodSize: NeighbourHoodSize){

    var dataModel:DataModel = new FileDataModel(new File(path))

    var similarity:UserSimilarity  =  (similarityType, dataModel);

    var neighborhood : UserNeighborhood  = new NearestNUserNeighborhood (neighbourHoodSize, similarity, dataModel);

    var recommender:Recommender = new GenericUserBasedRecommender (dataModel, neighborhood, similarity);


    def to(userId: UserId):UserBasedHelper  ={
    new UserBasedHelper(recommender,userId)
  }


  implicit def  similarityTypeToSimilarityObject(tuple: (SimilarityType,DataModel)): UserSimilarity ={
    tuple match {
      case (PEARSON_CORRELATION,dataModel:DataModel) => new PearsonCorrelationSimilarity(dataModel)
      case (EUCLIDEAN_DISTANCE,dataModel:DataModel) => new EuclideanDistanceSimilarity(dataModel)
    }
  }
}

case class UserBasedHelper(recommender:Recommender,userId:UserId){
  def recommends (numberOfItems: Int): List[RecommendedItem] ={
    asScalaBuffer(recommender.recommend(userId,numberOfItems)).toList

  }
}
