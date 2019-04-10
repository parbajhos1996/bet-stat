package yellowcardtype.yellowcard;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;
import utility.ContextWriter;

public class YellowCardCounterTest {

    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver = new MapDriver<>(new YellowCardCounterMapper());
    private ReduceDriver<Text, LongWritable, Text, DoubleWritable> reduceDriver = new ReduceDriver<>(new YellowCardCounterReducer());

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("E1,03/08/2018,Reading,Derby,1,2,A,0,0,D,R Jones,8,11,5,3,14,16,2,4,3,4,0"));
        mapDriver.withInput(new LongWritable(0), new Text("E1,03/08/2018,Chelsea,Arsenal,1,2,A,0,0,D,R Jones,8,11,5,3,14,16,2,4,5,7,0"));
        mapDriver.withInput(new LongWritable(0), new Text("E1,03/08/2018,Chelsea,Arsenal,1,2,A,0,0,D,R Jones,8,11,5,3,14,16,2,4,HY,AY,0"));

        mapDriver.withOutput(new Text("Reading"), new LongWritable(3));
        mapDriver.withOutput(new Text("Derby"), new LongWritable(4));
        mapDriver.withOutput(new Text("Chelsea"), new LongWritable(5));
        mapDriver.withOutput(new Text("Arsenal"), new LongWritable(7));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("Chelsea"), Arrays.asList(new LongWritable(5), new LongWritable(7)));

        reduceDriver.withOutput(new Text("Yellow card avg for match"), new DoubleWritable());
        reduceDriver.withOutput(new Text("_____________________________"), new DoubleWritable());
        reduceDriver.withOutput(new Text("Chelsea\t\t"), new DoubleWritable(12.0 / ContextWriter.PLAYED_MATCH_NO));

        reduceDriver.runTest();
    }
}
