package com.example;

/**
 * Created by viana on 04/03/15.
 */
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;

import java.io.*;


public class CollaborativeFiltering {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = (args.length > 0 && !args[0].isEmpty()) ? args[0] : "data/ua.base";
        String outputPath = (args.length > 1 && !args[1].isEmpty()) ? args[1] : "data/output";

        JavaRDD<String> data = sc.textFile(path);
        JavaRDD<Rating> ratings = data.map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split(",");
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2]));
                    }
                }
        );

        // Build the recommendation model using ALS
        int rank = 10;
        int numIterations = 20;
        double alpha = 0.01;
        long startTime = System.currentTimeMillis();

        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, alpha);

        long endTime = System.currentTimeMillis();

        PrintWriter writer;
        try {
		System.out.println("=================================Time for model training in Java Spark: "+ (endTime - startTime) + " milli seconds");
            writer = new PrintWriter(new FileOutputStream(new File(outputPath), false));
            writer.println("Time for model training in Java Spark: "+ (endTime - startTime)
                    + " milli seconds");
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
