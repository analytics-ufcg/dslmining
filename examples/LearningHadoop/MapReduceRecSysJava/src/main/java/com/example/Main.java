package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;


/**
 * Created by andryw on 12/02/15.
 */
public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputPath = (args.length > 0 && !args[0].isEmpty()) ? args[0] : "data/links-simple-1.txt";
        String outputPath = (args.length > 1 && !args[1].isEmpty()) ? args[1] : "data/";
        outputPath += "wikipedia_output/";
        String outputPathUsersVectors = outputPath + "users_vectores/";
        String outputPathCoOcurrence = outputPath + "coocurrence/";

        String tempPath =  outputPath + "/temp/";


        GenerateUserVectors(TextOutputFormat.class,inputPath,outputPathUsersVectors);

        GenerateUserVectors(SequenceFileOutputFormat.class,inputPath,tempPath);

        ComputeCoOcurrence(TextOutputFormat.class,tempPath,outputPathCoOcurrence);

        System.out.println("=================================================");
        System.out.println("To see the results of the first Map Reduce open the folder " + outputPathUsersVectors);
        System.out.println("To see the results of the second Map Reduce open the folder " + outputPathCoOcurrence);
        System.out.println("=================================================");


    }


    private static void GenerateUserVectors(Class outputFormatClass,String inputPath,String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        runJob("To User Vector",WikipediaToItemPrefsMapper.class,WikipediaToUserVectorReducer.class,
                VarLongWritable.class, VarLongWritable.class, VarLongWritable.class, VectorWritable.class,
                TextInputFormat.class,outputFormatClass,
                inputPath,outputPath);
    }

    private static void ComputeCoOcurrence(Class outputFormatClass,String inputPath,String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        runJob("To Coocurrence Matrix",UserVectorToCooccurrenceMapper.class,UserVectorToCooccurenceReduce.class,
                VarIntWritable.class, VarIntWritable.class, VectorWritable.class, VarIntWritable.class,
                SequenceFileInputFormat.class,outputFormatClass,
                inputPath,outputPath);
    }



    private static void runJob(String jobName,Class mapperClass, Class reducerClass, Class mapOutputKeyClass,
                               Class mapOutputValueClass, Class outputKeyClass, Class outputValueClass,
                               Class inputFormatClass, Class outputFormatClass, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = new Job(conf,jobName);

        //Set Mapper and Reducer Classes
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);

        //Set Map Output values.
        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);

        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);

        //Set the input and output.
        job.setInputFormatClass(inputFormatClass);
        job.setOutputFormatClass(outputFormatClass);

        //Delete the output path before run, to avoid exception
        FileSystem fs1 = FileSystem.get(conf);
        Path out1 = new Path(outputPath);
        fs1.delete(out1, true);

        //Set the input and output path
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    FileInputFormat.addInputPath(job, new Path(inputPath));
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    FileOutputFormat.setOutputPath(job, new Path(outputPath));


        job.waitForCompletion(true);
    }
}
