package yellowcardtype.opponentyellow;


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
 * This job averages how many yellow cards were given to the opponent team in a match.
 * For example if "Chelsea" played 3 matches with the following yellow card numbers:
 * First match yellow cards: 4
 * First match yellow cards: 5
 * First match yellow cards: 3
 * The average yellow cards will be 4.
 * This is good for yellow-card-type of betting.
 */
public class OpponentYellows extends Configured implements Tool {

    public static final String FILE_PATH = "hdfs://sandbox-hdp.hortonworks.com/tmp/bet/";
    public static final String OUTPUT_PATH = "hdfs://sandbox-hdp.hortonworks.com/tmp/oppyellows";

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("Opponent Yellow Card counter");

        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(OpponentYellowCounterMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setReducerClass(OpponentYellowCounterReducer.class);

        FileInputFormat.addInputPath(job, new Path(FILE_PATH + strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new OpponentYellows(), new String[] { args[0] });
    }
}
