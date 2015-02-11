package com.example.libimset;

import com.example.WithAnonymousRecommender;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.RandomRecommender;
import org.apache.mahout.cf.taste.model.DataModel;

import java.io.File;
import java.io.IOException;

public class LibimsetRecommenderMain {

    public static void main(String... args) throws TasteException, IOException {
        DataModel model = new FileDataModel(new File("data/libimset-ratings.dat"));

        RecommenderBuilder userBasedBuilder = dataModel -> new WithAnonymousRecommender(_dataModel -> {
            try {
                return new LibimsetRecommender(_dataModel, "data/libimset-gender.dat");
            } catch (IOException e) {
                return null;
            }
        }, dataModel);

        RecommenderBuilder randomBuilder = dataModel -> new RandomRecommender(dataModel);

        RecommenderEvaluator evaluator = new RMSRecommenderEvaluator();
        double libimsetScore = evaluator.evaluate(userBasedBuilder, null, model, 0.95, 0.05);
        double randomScore = evaluator.evaluate(randomBuilder, null, model, 0.95, 0.05);

        System.out.println("libimsetScore = " + libimsetScore);
        System.out.println("randomScore = " + randomScore);
    }
}