/**
 * Created by viana on 05/03/15.
 */
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

import java.io.*;


public class CollaborativeFilteringEvaluator {

    public static void main(String... args) throws TasteException, IOException {

        String path = (args.length > 0 && !args[0].isEmpty()) ? args[0] : "data/ua.base";
        String outputPath = (args.length > 1 && !args[1].isEmpty()) ? args[1] : "data/output";

        DataModel model = new FileDataModel(new File(path));

        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model)
                    throws TasteException {
                return
                        new SVDRecommender(model, new ALSWRFactorizer(model, 10, 0.01, 20));
            }
        };

        RecommenderEvaluator evaluator = new RMSRecommenderEvaluator();
        double rmsr = evaluator.evaluate(recommenderBuilder, null, model, 0.9, 1);
        
        PrintWriter writer;
        try {
            writer = new PrintWriter(outputPath, "UTF-8");
            writer.println("Root Mean Square Residual = " + rmsr);
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


    }
}