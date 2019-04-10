package goaltype.goalconcede;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import utility.ContextWriter;
import yellowcardtype.yellowcard.YellowCardCounterReducer;

public class GoalsConcededTest {

    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver = new MapDriver<>(new GoalsConcededCounterMapper());
    private ReduceDriver<Text, LongWritable, Text, DoubleWritable> reduceDriver = new ReduceDriver<>(new GoalsConcededCounterReducer());

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("E1,03/08/2018,Reading,Derby,1,2"));
        mapDriver.withInput(new LongWritable(0), new Text("E1,03/08/2018,Chelsea,Arsenal,5,4"));
        mapDriver.withInput(new LongWritable(0), new Text("E1,03/08/2018,City,United,6,0"));
        mapDriver.withInput(new LongWritable(0), new Text("E1,03/08/2018,City,United,FTHG,FTAG"));

        mapDriver.withOutput(new Text("Reading"), new LongWritable(2));
        mapDriver.withOutput(new Text("Derby"), new LongWritable(1));
        mapDriver.withOutput(new Text("Chelsea"), new LongWritable(4));
        mapDriver.withOutput(new Text("Arsenal"), new LongWritable(5));
        mapDriver.withOutput(new Text("City"), new LongWritable(0));
        mapDriver.withOutput(new Text("United"), new LongWritable(6));


        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("Chelsea"), Arrays.asList(new LongWritable(5), new LongWritable(7)));

        reduceDriver.withOutput(new Text("Avg goals conceded per match"), new DoubleWritable());
        reduceDriver.withOutput(new Text("_____________________________"), new DoubleWritable());
        reduceDriver.withOutput(new Text("Chelsea\t\t"), new DoubleWritable(12.0 / ContextWriter.PLAYED_MATCH_NO));

        reduceDriver.runTest();
    }
}
