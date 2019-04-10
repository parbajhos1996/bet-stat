package winnertype.avgwinnerodd;

import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utility.ContextWriter;

public class AvgWinnerOddMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        if(isProcessable(split)) {
            int homeGoals = Integer.parseInt(split[4]);
            int awayGoals = Integer.parseInt(split[5]);
            if(homeGoals > awayGoals) {
                context.write(new Text(split[2]), new DoubleWritable(Double.parseDouble(split[23])));
            } else if(awayGoals > homeGoals) {
                context.write(new Text(split[3]), new DoubleWritable(Double.parseDouble(split[25])));
            }
        }
    }

    private boolean isProcessable(String[] split) {
        return split.length >= 26 && !split[23].equals("B365H");
    }
}
