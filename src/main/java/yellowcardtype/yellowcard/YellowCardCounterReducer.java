package yellowcardtype.yellowcard;

import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utility.ContextWriter;

public class YellowCardCounterReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        ContextWriter.writeContextSetup("Yellow card avg for match", context, Double.class);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        double result = 0L;
        for(LongWritable number : values) {
            result += number.get();
        }
        ContextWriter.writeContext(key, context, result / ContextWriter.PLAYED_MATCH_NO);
    }

}
