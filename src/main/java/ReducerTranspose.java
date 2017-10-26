
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class ReducerTranspose extends Reducer<LongWritable, MapWritable, LongWritable, Text> {

    public void reduce(LongWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

        SortedMap<LongWritable,IntWritable> rowValues = new TreeMap<LongWritable,IntWritable>();

        //Iterate on values to attribute each val to row
        for (MapWritable val : values) {
            for(Entry<Writable, Writable>  entry : val.entrySet()) {
                rowValues.put((LongWritable) entry.getKey(),(IntWritable) entry.getValue());
            }
        }

        //Create a string by concatenation
        StringBuffer sb = new StringBuffer();
        for(IntWritable rowVal : rowValues.values()) {
            sb.append(rowVal.toString() + ",");
        }

        //Write the key and the value in the context
        context.write(key, new Text(sb.toString()));
    }
}
