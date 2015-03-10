package com.example;

import com.example.libimset.LibimsetRecommender;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.RandomRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;

public class LibimsetRecommenderMain {

    public static void main(String... args) throws TasteException, IOException {
        DataModel model = new FileDataModel(new File("data/libimset-ratings.dat"));


        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model)
                    throws TasteException {
                UserSimilarity similarity =  new PearsonCorrelationSimilarity(model);
                UserNeighborhood neighborhood = new NearestNUserNeighborhood(10, similarity, model);
                return
                        new GenericUserBasedRecommender(model, neighborhood, similarity);
            }
        };

        RecommenderBuilder randomBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                return new RandomRecommender(dataModel);
            }
        };

        RecommenderEvaluator evaluator = new RMSRecommenderEvaluator();
        double libimsetScore = evaluator.evaluate(recommenderBuilder, null, model, 0.95, 0.05);
        double randomScore = evaluator.evaluate(randomBuilder, null, model, 0.95, 0.05);

        System.out.println("libimsetScore = " + libimsetScore);
        System.out.println("randomScore = " + randomScore);
    }
}