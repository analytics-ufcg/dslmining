package com.example;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.io.File;
import java.io.IOException;

public class CollaborativeFiltering {

    public static void main(String... args) throws TasteException, IOException {
        DataModel model = new FileDataModel(new File("data/ml-100k/ua.base"));

        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model)
                    throws TasteException {
                return
                        new SVDRecommender(model, new ALSWRFactorizer(model, 10, 0.01, 20));
            }
        };

        RecommenderEvaluator evaluator = new RMSRecommenderEvaluator();
        double Score = evaluator.evaluate(recommenderBuilder, null, model, 0.9, 1);

        System.out.println("Score = " + Score);
    }
}