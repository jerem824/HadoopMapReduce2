
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperTranspose extends Mapper<LongWritable, Text, LongWritable, MapWritable> {

    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //Split csv file by ","
        String[] table = value.toString().split(",");

        MapWritable map = new MapWritable();

        //Return for each element: (column, (row, value))
        int column = 0;
        for(String val : table)
        {
            map.put(key, new IntWritable(Integer.parseInt(val)));
            context.write(new LongWritable(column), map);
            column++;
        }
    }
}


