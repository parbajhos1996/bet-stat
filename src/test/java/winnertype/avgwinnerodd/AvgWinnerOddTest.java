package winnertype.avgwinnerodd;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class AvgWinnerOddTest {

    private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver = new MapDriver<>(new AvgWinnerOddMapper());
    private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver = new ReduceDriver<>(new AvgWinnerOddReducer());

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(0),
                new Text("E1,03/08/2018,Reading,Derby,1,2,A,0,0,D,R Jones,8,11,5,3,14,16,2,4,3,4,0,0,2.0,3.0,4.0"));
        mapDriver.withInput(new LongWritable(0),
                new Text("E1,03/08/2018,Chelsea,Arsenal,5,4,A,0,0,D,R Jones,8,11,5,3,14,16,2,4,5,7,0,0,3.0,4.0,2.5"));
        mapDriver.withInput(new LongWritable(0),
                new Text("E1,03/08/2018,Chelsea,Arsenal,1,2,A,0,0,D,R Jones,8,11,5,3,14,16,2,4,5,7,0,0,B365H,B365D,B365A"));

        mapDriver.withOutput(new Text("Derby"), new DoubleWritable(4.0));
        mapDriver.withOutput(new Text("Chelsea"), new DoubleWritable(3.0));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("Chelsea"), Arrays.asList(new DoubleWritable(5.2), new DoubleWritable(7.8)));
        int size = 2;

        reduceDriver.withOutput(new Text("Avg winner odd"), new DoubleWritable());
        reduceDriver.withOutput(new Text("_____________________________"), new DoubleWritable());
        reduceDriver.withOutput(new Text("Chelsea\t\t"), new DoubleWritable(13.0 / size));

        reduceDriver.runTest();
    }
}
