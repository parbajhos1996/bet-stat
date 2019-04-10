package yellowcardtype.yellowcard;

import java.io.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YellowCardCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        if(split.length >= 21) {
            if(!split[19].equals("HY") && !split[20].equals("AY")) {
                context.write(new Text(split[2]), new LongWritable(Long.parseLong(split[19])));
                context.write(new Text(split[3]), new LongWritable(Long.parseLong(split[20])));
            }
        }
    }
}
