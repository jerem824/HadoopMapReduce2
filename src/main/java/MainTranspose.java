
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainTranspose {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //Creation of configuration
        Configuration conf = new Configuration(true);

        //Creation of job
        Job job = Job.getInstance(conf, "Transpose");
        job.setJarByClass(MainTranspose.class);

        //Setup the Mapper and Reducer functions
        job.setMapperClass(MapperTranspose.class);
        job.setReducerClass(ReducerTranspose.class);

        //Specify key and value
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(MapWritable.class);

        //Add Input and Output Paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Delete output if exists
        if (FileSystem.get(conf).exists(new Path(args[1]))){
            FileSystem.get(conf).delete(new Path(args[1]), true);
        }

        //Execute job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


