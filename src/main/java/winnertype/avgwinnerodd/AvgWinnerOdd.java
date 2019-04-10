package winnertype.avgwinnerodd;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This job averages the odds provided by bet365 when the given team wins.
 * For example if "Chelsea" played 3 matches:
 * Chelsea - Arsenal 2-0           Odd: 2,0
 * Crystal Palace - Chelsea 1-0    Odd: 1,5
 * Chelsea - MU 2-1                Odd: 2,6
 * The average winner odd will be 2,3, because Chelsea won 2 matches with 2,0 and 2,6 odds, so the average is 2,3.
 * This is a good stat for knowing when it's worth to bet on the team.
 */
public class AvgWinnerOdd extends Configured implements Tool {

    public static final String FILE_PATH = "hdfs://sandbox-hdp.hortonworks.com/tmp/bet/";
    public static final String OUTPUT_PATH = "hdfs://sandbox-hdp.hortonworks.com/tmp/avgwinnerodd";

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("Avarage odd when team wins");

        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(AvgWinnerOddMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setReducerClass(AvgWinnerOddReducer.class);

        FileInputFormat.addInputPath(job, new Path(FILE_PATH + strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new AvgWinnerOdd(), new String[] { args[0] });
    }
}
