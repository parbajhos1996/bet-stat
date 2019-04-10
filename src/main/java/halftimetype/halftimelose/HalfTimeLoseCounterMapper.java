package halftimetype.halftimelose;

import java.io.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HalfTimeLoseCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        if(isProcessable(split)) {
            int homeHalfGoals = Integer.parseInt(split[7]);
            int awayHalfGoals = Integer.parseInt(split[8]);
            if(homeHalfGoals > awayHalfGoals) {
                context.write(new Text(split[3]), new LongWritable(1));
            } else if (awayHalfGoals > homeHalfGoals) {
                context.write(new Text(split[2]), new LongWritable(1));
            }
        }
    }

    private boolean isProcessable(String[] split) {
        return split.length >= 9 && !split[7].equals("HTHG");
    }
}
