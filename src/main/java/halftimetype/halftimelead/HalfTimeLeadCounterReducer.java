package halftimetype.halftimelead;

import java.io.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utility.ContextWriter;

public class HalfTimeLeadCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        ContextWriter.writeContextSetup("Half-time leading count", context, Long.class);
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
