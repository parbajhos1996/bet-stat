package winnertype.avgwinnerodd;

import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utility.ContextWriter;

public class AvgWinnerOddReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        ContextWriter.writeContextSetup("Avg winner odd", context, Double.class);
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double result = 0L;
        int count = 0;
        for(DoubleWritable number : values) {
            result += number.get();
            count++;
        }
        ContextWriter.writeContext(key, context, result / count);
    }
}
