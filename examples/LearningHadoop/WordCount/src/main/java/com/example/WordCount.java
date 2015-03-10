package com.example;

/**
 * Created by andryw on 24/02/15.
 */
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    //Class for the Map phase
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            //For each word, emits the word (key) and the number 1 (value)
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    //Class for the Reduce phase
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            //Sum all occurrences (list of values) of the word (key)
            for (IntWritable val : values) {
                sum += val.get();
            }

            //Emit the word (key) and the total of occurrences (value)
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        //Set input and output path by the args, or use default paths
        String inputPath = (args.length > 0 && !args[0].isEmpty()) ? args[0] : "data/words.txt";
        String outputPath = (args.length > 1 && !args[1].isEmpty()) ? args[1] : "data/wordcount/";

        Configuration conf = new Configuration();
        Job job = new Job(conf, "wordcount");

        //Set Output values.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Set Mapper and Reducer Classes
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //Set the input and output. It reads a text file and saves on a text file.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Set the input and output path
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //Delete the output path before run, to avoid exception
        FileSystem fs1 = FileSystem.get(conf);
        Path out1 = new Path(outputPath);
        fs1.delete(out1, true);

        //Run!
        job.waitForCompletion(true);
        System.out.println("=================================================");
        System.out.println("To see the results open the folder " + outputPath);
        System.out.println("=================================================");

    }

}
