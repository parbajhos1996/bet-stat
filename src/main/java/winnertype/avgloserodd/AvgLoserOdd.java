package winnertype.avgloserodd;


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
import winnertype.avgwinnerodd.AvgWinnerOddMapper;
import winnertype.avgwinnerodd.AvgWinnerOddReducer;

/**
 * This job averages the odds provided by bet365 when the given team loses.
 * For example if "Chelsea" played 3 matches:
 * Chelsea - Arsenal 0-2           Odd: 2,0
 * Crystal Palace - Chelsea 1-0    Odd: 1,5
 * Chelsea - MU 2-1                Odd: 2,6
 * The average winner odd will be 1.75, because Chelsea lost 2 matches with 2,0 and 1,5 odds.
 * This is a good stat for knowing when it's not worth to bet on the team.
 */
public class AvgLoserOdd extends Configured implements Tool {

    public static final String FILE_PATH = "hdfs://sandbox-hdp.hortonworks.com/tmp/bet/";
    public static final String OUTPUT_PATH = "hdfs://sandbox-hdp.hortonworks.com/tmp/avgloserodd";

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("Avarage odd when team loses");

        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(AvgLoserOddMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setReducerClass(AvgLoserOddReducer.class);

        FileInputFormat.addInputPath(job, new Path(FILE_PATH + strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new AvgLoserOdd(), new String[] { args[0] });
    }
}
