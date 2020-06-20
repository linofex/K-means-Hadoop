package it.unipi.hadoop.Kmeans;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.NullWritable;

public class Kmean{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: Kmeans <input> <output> <number of centroids>");
            System.exit(1);
        }
       

        System.out.println("args[0]: <input>="+otherArgs[0]);
        System.out.println("args[1]: <output>="+otherArgs[1]);
        System.out.println("args[2]: <number of centroids>="+otherArgs[2]); 

        // generazione centroidi iniziali
        // caricare il file su HDFS + definire anche il nome del file
        // centroidsFileName = nome del file dei centroidi

        conf.setDouble("threshold", 0.1);
        conf.setStrings("centroidsFilePath", "centroids.txt");

        int iteration = 1;
        int max_iterations = 20;
        Job job;
        Boolean isConverged = false;
        while(!isConverged){
            job = Job.getInstance(conf, "Kmeans");
            
            // job.setJarByClass(job.getClass()); 
            job.setJarByClass(Kmean.class); 

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

            job.waitForCompletion(true);
            long counter = job.getCounters().findCounter(CentroidCounter.NUMBER_OF_UNCONVERGED).getValue();
            isConverged = (counter == 0 || iteration > max_iterations);
            
            if (!isConverged)
                fs.delete(new Path(otherArgs[1]), true);
            System.out.println("Counter: " + counter);
            System.out.println(("Iteration: ") + iteration);
            iteration++;

        }
        System.out.println();
    }
    
}
