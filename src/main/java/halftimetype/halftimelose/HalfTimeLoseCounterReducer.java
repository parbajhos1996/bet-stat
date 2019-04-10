package halftimetype.halftimelose;

import java.io.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utility.ContextWriter;

public class HalfTimeLoseCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    public static final int PLAYED_MATCH_NO = 40;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        ContextWriter.writeContextSetup("Half-time trailing count", context, Long.class);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long result = 0L;
        for(LongWritable number : values) {
            result++;
        }
        ContextWriter.writeContext(key, context, result);
    }
}
