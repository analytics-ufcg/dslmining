package com.example;


import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by andryw on 20/02/15.
 */
public class UserVectorToCooccurenceReduce extends Reducer<VarIntWritable,VarIntWritable,VarIntWritable,VectorWritable> {

    public void reduce(VarIntWritable itemIndex1, Iterable<VarIntWritable> itemIndex2s, Context context) throws IOException, InterruptedException {
        Vector cooccureenceRow = new RandomAccessSparseVector(Integer.MAX_VALUE,100);
        for (VarIntWritable indexWritable2 : itemIndex2s){
            int index2 = indexWritable2.get();
            cooccureenceRow.set(index2,cooccureenceRow.get(index2)+1);
        }
        context.write(itemIndex1,new VectorWritable(cooccureenceRow));
    }
}
