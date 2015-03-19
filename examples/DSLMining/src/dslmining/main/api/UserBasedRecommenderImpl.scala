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

//Class that receives the parameters to create a UserBased Recommender.
case class UserBasedRecommenderImpl (val path: WithPath, val recomType: RecommendationType, val similarityType: SimilarityType, val neighbourHoodSize: NeighbourHoodSize){

    var dataModel:DataModel = new FileDataModel(new File(path))

    var similarity:UserSimilarity  =  (similarityType, dataModel);

    var neighborhood : UserNeighborhood  = new NearestNUserNeighborhood (neighbourHoodSize, similarity, dataModel);

    var recommender:Recommender = new GenericUserBasedRecommender (dataModel, neighborhood, similarity);

    //Function that receiveis the userID that will receive the recommendations. The alias is 'to' because 'to userID recommends numberOfItems'
    //The class UserBasedHelper will make the recommendations
    def to(userId: UserId):UserBasedHelper  = {
      new UserBasedHelper(recommender,userId)
  }

  //Implicitly check the SimilarityType and returns a UserSimiliarity (of Mahout) object
  implicit def  similarityTypeToSimilarityObject(tuple: (SimilarityType,DataModel)): UserSimilarity ={
    tuple match {
      case (PEARSON_CORRELATION,dataModel:DataModel) => new PearsonCorrelationSimilarity(dataModel)
      case (EUCLIDEAN_DISTANCE,dataModel:DataModel) => new EuclideanDistanceSimilarity(dataModel)
    }
  }
}

//Class that receives a recommender and a userID to recommends to him a numberOfItems recommendations.
case class UserBasedHelper(recommender:Recommender,userId:UserId){

  //Function thar recomends to the userID a numberOfItems recommendations
  def recommends (numberOfItems: Int): List[RecommendedItem] = {
    //As recommender returns a Java List, asScalaBuffer convert this list to a Scala Buffer.
    asScalaBuffer(recommender.recommend(userId,numberOfItems)).toList

  }
}
