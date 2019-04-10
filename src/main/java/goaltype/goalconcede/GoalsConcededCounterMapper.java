package goaltype.goalconcede;

import java.io.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GoalsConcededCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        if(isProcessable(split)) {
            context.write(new Text(split[2]), new LongWritable(Long.parseLong(split[5])));
            context.write(new Text(split[3]), new LongWritable(Long.parseLong(split[4])));
        }
    }

    private boolean isProcessable(String[] split) {
        return split.length >= 6 && !split[4].equals("FTHG");
    }
}
