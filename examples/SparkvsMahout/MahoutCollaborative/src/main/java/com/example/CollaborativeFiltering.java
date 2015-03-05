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

public class CollaborativeFiltering {

    public static void main(String... args) throws TasteException, IOException {

        String path = (args.length > 0 && !args[0].isEmpty()) ? args[0] : "data/ua.base";
        String outputPath = (args.length > 1 && !args[1].isEmpty()) ? args[1] : "data/output";

        DataModel model = new FileDataModel(new File(path));

        long startTime = System.currentTimeMillis();

        SVDRecommender svdRecommender = new SVDRecommender(model, new ALSWRFactorizer(model, 10, 0.01, 20));

        long endTime = System.currentTimeMillis();

        PrintWriter writer;
        try {
            writer = new PrintWriter(outputPath, "UTF-8");
            writer.println("Time for run CollaborativeFiltering in Java Mahout: "+ (endTime - startTime)
                    + " milli seconds");
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }


    }
}