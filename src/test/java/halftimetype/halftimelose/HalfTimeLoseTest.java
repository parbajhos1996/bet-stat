package halftimetype.halftimelose;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class HalfTimeLoseTest {

    private MapDriver<LongWritable, Text, Text, LongWritable> mapDriver = new MapDriver<>(new HalfTimeLoseCounterMapper());
    private ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver = new ReduceDriver<>(new HalfTimeLoseCounterReducer());

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(0), new Text("E1,03/08/2018,Reading,Derby,1,2,A,1,0"));
        mapDriver.withInput(new LongWritable(0), new Text("E1,03/08/2018,Chelsea,Arsenal,1,2,A,2,0,"));
        mapDriver.withInput(new LongWritable(0), new Text("E1,03/08/2018,Chelsea,Arsenal,1,2,A,HTHG,HTAG"));

        mapDriver.withOutput(new Text("Derby"), new LongWritable(1));
        mapDriver.withOutput(new Text("Arsenal"), new LongWritable(1));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("Chelsea"), Arrays.asList(new LongWritable(1), new LongWritable(1)));

        reduceDriver.withOutput(new Text("Half-time trailing count"), new LongWritable());
        reduceDriver.withOutput(new Text("_____________________________"), new LongWritable());
        reduceDriver.withOutput(new Text("Chelsea\t\t"), new LongWritable(2));

        reduceDriver.runTest();
    }
}
