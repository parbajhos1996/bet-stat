package utility;

import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ContextWriter {

    public static final int PLAYED_MATCH_NO = 40;

    public static <T> void writeContextSetup(String title, Context context, Class<T> clazz) throws IOException, InterruptedException {
        Writable writable = getWritableByClass(clazz);
        context.write(new Text(title), writable);
        context.write(new Text("_____________________________"), writable);
    }

    public static <T> void writeContext(Text key, Context context, T result) throws IOException, InterruptedException {
        Writable writable = getWritable(result);
        if (key.toString().length() <= 7) {
            context.write(new Text(key.toString() + "\t" + "\t"), writable);
        } else {
            context.write(new Text(key.toString() + "\t"), writable);
        }
    }

    private static <T> Writable getWritable(T result) {
        Writable writable;
        if(result instanceof Double) {
            writable = new DoubleWritable((Double) result);
        } else {
            writable = new LongWritable((Long) result);
        }
        return writable;
    }

    private static <T> Writable getWritableByClass(Class<T> result) {
        Writable writable;
        if(result.getCanonicalName().equals("java.lang.Double")) {
            writable = new DoubleWritable();
        } else {
            writable = new LongWritable();
        }
        return writable;
    }


}
