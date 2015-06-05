package api;

import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.hadoop.ToEntityPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.ToItemPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexMapper;
import org.apache.mahout.cf.taste.hadoop.item.ItemIDIndexReducer;
import org.apache.mahout.cf.taste.hadoop.item.ToUserVectorsReducer;
import org.apache.mahout.cf.taste.hadoop.item.ToUserVectorsReducer.Counters;
import org.apache.mahout.cf.taste.hadoop.preparation.ToItemVectorsMapper;
import org.apache.mahout.cf.taste.hadoop.preparation.ToItemVectorsReducer;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

public class PreparePreferenceMatrixJob extends AbstractJob {
    public static final String NUM_USERS = "numUsers.bin";
    public static final String ITEMID_INDEX = "itemIDIndex";
    public static final String USER_VECTORS = "userVectors";
    public static final String RATING_MATRIX = "ratingMatrix";
    private static final int DEFAULT_MIN_PREFS_PER_USER = 1;

    public PreparePreferenceMatrixJob() {
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PreparePreferenceMatrixJob(), args);
    }

    public int run(String[] args) throws Exception {
        this.addInputOption();
        this.addOutputOption();
        this.addOption("minPrefsPerUser", "mp", "ignore users with less preferences than this (default: 1)", String.valueOf(1));
        this.addOption("booleanData", "b", "Treat input as without pref values", Boolean.FALSE.toString());
        this.addOption("ratingShift", "rs", "shift ratings by this value", "0.0");
        Map parsedArgs = this.parseArguments(args);
        if(parsedArgs == null) {
            return -1;
        } else {
            int minPrefsPerUser = Integer.parseInt(this.getOption("minPrefsPerUser"));
            boolean booleanData = Boolean.valueOf(this.getOption("booleanData")).booleanValue();
            float ratingShift = Float.parseFloat(this.getOption("ratingShift"));
            Job itemIDIndex = this.prepareJob(this.getInputPath(), this.getOutputPath("itemIDIndex"),
                    TextInputFormat.class, ItemIDIndexMapper.class, VarIntWritable.class, VarLongWritable.class,
                    ItemIDIndexReducer.class, VarIntWritable.class, VarLongWritable.class, TextOutputFormat.class);
            itemIDIndex.setCombinerClass(ItemIDIndexReducer.class);
            boolean succeeded = itemIDIndex.waitForCompletion(true);
            if(!succeeded) {
                return -1;
            } else {
                Job toUserVectors = this.prepareJob(this.getInputPath(), this.getOutputPath("userVectors"),
                        TextInputFormat.class, ToItemPrefsMapper.class, VarLongWritable.class,
                        booleanData?VarLongWritable.class:EntityPrefWritable.class,
                        ToUserVectorsReducer.class, VarLongWritable.class,
                        VectorWritable.class, TextOutputFormat.class);
                toUserVectors.getConfiguration().setBoolean("booleanData", booleanData);
                toUserVectors.getConfiguration().setInt(ToUserVectorsReducer.MIN_PREFERENCES_PER_USER, minPrefsPerUser);
                toUserVectors.getConfiguration().set(ToEntityPrefsMapper.RATING_SHIFT, String.valueOf(ratingShift));
                succeeded = toUserVectors.waitForCompletion(true);
                if(!succeeded) {
                    return -1;
                } else {
                    int numberOfUsers = (int)toUserVectors.getCounters().
                            findCounter(ToUserVectorsReducer.Counters.USERS).getValue();

                    HadoopUtil.writeInt(numberOfUsers, this.getOutputPath("numUsers.bin"), this.getConf());
                    Job toItemVectors = this.prepareJob(this.getOutputPath("userVectors"),
                            this.getOutputPath("ratingMatrix"), ToItemVectorsMapper.class,
                            IntWritable.class, VectorWritable.class, ToItemVectorsReducer.class,
                            IntWritable.class, VectorWritable.class);

                    toItemVectors.setCombinerClass(ToItemVectorsReducer.class);
                    succeeded = toItemVectors.waitForCompletion(true);
                    return !succeeded?-1:0;
                }
            }
        }
    }
}

