package com.example;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import javax.naming.Context;
import java.io.IOException;

/**
 * Created by andryw on 19/02/15.
 */
public class WikipediaToUserVectorReducer extends Reducer<VarLongWritable,VarLongWritable,VarLongWritable,VectorWritable> {

    public void reduce(VarLongWritable userID,Iterable<VarLongWritable> itemPrefs, Context context) throws IOException, InterruptedException {
        Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);

        for (VarLongWritable itemPref : itemPrefs) {
            userVector.set((int)itemPref.get(), 1.0f);
        }
       // System.out.println(userID + " " +userVector);
        VectorWritable vw = new VectorWritable(userVector);
        context.write(userID, vw);
    }

}
