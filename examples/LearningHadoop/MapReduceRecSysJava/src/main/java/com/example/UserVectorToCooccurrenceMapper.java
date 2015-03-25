package com.example;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by andryw on 10/02/15.
 */
public class UserVectorToCooccurrenceMapper extends Mapper<VarLongWritable,VectorWritable,VarIntWritable,VarIntWritable> {

    public void map(VarLongWritable userID, VectorWritable userVector, Context context) throws IOException, InterruptedException {
        Iterator<Vector.Element> it = userVector.get().iterateNonZero();
        while (it.hasNext()){
            int index1 = it.next().index();
            Iterator<Vector.Element> it2 = userVector.get().iterateNonZero();
            while (it2.hasNext()){
                int index2 = it2.next().index();
                context.write(new VarIntWritable(index1), new VarIntWritable(index2));
            }

        }
    }
}
