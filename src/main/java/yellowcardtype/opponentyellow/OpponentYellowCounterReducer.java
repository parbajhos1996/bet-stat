package yellowcardtype.opponentyellow;

import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utility.ContextWriter;

public class OpponentYellowCounterReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

    public static final int PLAYED_MATCH_NO = 40;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        ContextWriter.writeContextSetup("Opponent yellow cards avg", context, Double.class);
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
