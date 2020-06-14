package Kmeans.src;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Kmean{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Kmeans <input> <output> <number of centroids>");
            System.exit(1);
        }
        conf.setDouble("threshold",1.0);

        // generazione centroidi su 

        System.out.println("args[0]: <input>="+otherArgs[0]);
        System.out.println("args[1]: <output>="+otherArgs[1]);
        System.out.println("args[2]: <number of centroids>="+otherArgs[2]);



        Job job = Job.getInstance(conf, "Kmeans");
        job.setJarByClass(getClass()); 

        // set mapper/combiner/reducer
        job.setMapperClass(KmeansMapper.class);
        job.setCombinerClass(KmeansCombiner.class);
        job.setReducerClass(KmeansReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Mean.class);
        job.setMapOutputValueClass(Point.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class); 
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1); // da iterare
    }
}
