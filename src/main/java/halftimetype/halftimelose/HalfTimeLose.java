package halftimetype.halftimelose;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This job counts how many times the given team was trailing after halt-time.
 * For example if "Chelsea" played 3 matches with the following half-time result:
 * Chelsea - Arsenal 2-0           Half-time result: Leading
 * Chelsea - MU 2-1                Half-time result: Leading
 * Crystal Palace - Chelsea 1-0    Half-time result: Losing
 * The count will be 1.
 * This is good for half-time-type of betting.
 */
public class HalfTimeLose extends Configured implements Tool {

    public static final String FILE_PATH = "hdfs://sandbox-hdp.hortonworks.com/tmp/bet/";

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("Half time lead count");

        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(HalfTimeLoseCounterMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setReducerClass(HalfTimeLoseCounterReducer.class);

        FileInputFormat.addInputPath(job, new Path(FILE_PATH + strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH()));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private String OUTPUT_PATH() {
        return "hdfs://sandbox-hdp.hortonworks.com/tmp/halftimelose";
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new HalfTimeLose(), new String[] { args[0] });
    }
}
