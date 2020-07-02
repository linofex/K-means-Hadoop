package it.unipi.hadoop.Kmeans;


import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;
import java.util.HashSet;
import java.util.Set;

import com.ctc.wstx.util.StringUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.NullWritable;

public class Kmean{

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 6) {
            System.err.println("Usage: Kmeans <input> <output> <number of centroids or file> <max iter> <threshold> <num reducers>");
            System.exit(1);
        }
       
        // input output k max_it the
        // System.out.println("args[0]: <input>="+otherArgs[0]);
        // System.out.println("args[1]: <output>="+otherArgs[1]);
        // System.out.println("args[2]: <number of centroids>="+otherArgs[2]); 

        Path inputPath = new Path(otherArgs[0]);
        Path outpPath = new Path(otherArgs[1]);
        double threshold = Double.parseDouble(otherArgs[4]);
        String centroids = otherArgs[2];
        int max_iterations = Integer.parseInt(otherArgs[3]);
        int numReducers = Integer.parseInt(otherArgs[5]);
        Path centroidsPath = null;
        if(StringUtils.isNumeric(centroids)){ // generate random centroids from input points
            centroidsPath = generateCentroids(fs,Integer.parseInt(centroids), inputPath);
            centroids = centroidsPath.toString();
        }
        else
            centroidsPath = new Path(centroids);
    
        conf.setDouble("threshold", threshold);
        conf.setStrings("centroidsFilePath", centroids);
        
        int iteration = 1;
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
            job.setOutputValueClass(Text.class);
            
            job.setNumReduceTasks(numReducers);

            // define I/O
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outpPath);

            job.setInputFormatClass(TextInputFormat.class); 
            job.setOutputFormatClass(TextOutputFormat.class);

            job.waitForCompletion(true);

            //cambiare file con i centroidi, cos
            if(iteration  == 1){ //first iteration
                centroids = centroids.split("\\.")[0]+"_final.txt";
                conf.setStrings("centroidsFilePath",centroids );
                centroidsPath = new Path(centroids);
            }
            
            // compute new centroids
            computeNewCentroids(fs, centroidsPath , outpPath);
                        
            long counter = job.getCounters().findCounter(CentroidCounter.NUMBER_OF_UNCONVERGED).getValue();
            isConverged = (counter == 0 || iteration > max_iterations);
            // isConverged = true;
            if (!isConverged)
                fs.delete(new Path(otherArgs[1]), true);
            System.out.println("Current iteration: " + iteration);
            System.out.println("Unconverged centroids: " + counter);
            iteration++;
        }
        System.out.println();
    }

    private static void computeNewCentroids(FileSystem fs, Path centroids, Path outPath) throws FileNotFoundException, IOException{
        BufferedWriter bw =  new BufferedWriter( new OutputStreamWriter(fs.create(centroids, true))); //override if exist
        // retreive all part-r-* files created by the reducer(s)
        FileStatus[] outfiles = fs.listStatus(outPath, (file -> file.getName().startsWith("part-r-")));
        // concatenate all the part-r-* files generated by the reducer and write them to the centroids.txt file
        BufferedReader br = null;
        for (int i = 0; i < outfiles.length; ++i){
            br = new BufferedReader(new InputStreamReader(fs.open(outfiles[i].getPath())));
            String line = br.readLine();
            while (line != null){
                bw.write(line+"\n");   
                line = br.readLine(); 
            }
            br.close();
        }
        bw.close();
    }

    private static Path generateCentroids(FileSystem fs, int num_centroids, Path inputPath) throws IOException{
        //ArrayList<Integer> random_points_indexes = new ArrayList<Integer>(); 
        int pointNum = Integer.parseInt(inputPath.toString().split("_")[1]);
        Path centroidPath = new Path("initial_means/random_centroids.txt");
        int lineCounter = 0;
        int clusterIdCounter = 1;
        Random randNum = new Random();
        
        // choose num_centroids random indeces with no duplicates (Random seeds with current time)
        Set<Integer> random_points_indexes = new HashSet<Integer>();
        while (random_points_indexes.size() < num_centroids) {
            random_points_indexes.add(randNum.nextInt(pointNum)+1);
        }

        // write the randomly chosen centroids into the "random_centroids.txt" file
        try{
            BufferedReader br = new BufferedReader (new InputStreamReader(fs.open(inputPath)));
            BufferedWriter bw =  new BufferedWriter( new OutputStreamWriter(fs.create(centroidPath, true))); //override if exist
            String line = br.readLine();
            //read the points
            while (line != null){  
                if(random_points_indexes.contains(lineCounter)){
                    bw.write(clusterIdCounter + "\t" + line + "\n");
                    clusterIdCounter ++;
                    if(clusterIdCounter > num_centroids) // to avoid complete read of the file when the task is finished
                        break;
                }
                line = br.readLine();
                lineCounter++;
            }
            br.close();
            /*
            int point_num = points.size();
            Random objGenerator = new Random();
            // choose num_centroids random points and set them as initial centroids.
            for (int i = 1; i <= num_centroids; i++){
                int randomPoint = objGenerator.nextInt(point_num + 1);
                bw.write(i+"\t"+points.get(randomPoint)+"\n");
            }
            */
            bw.close();
        }
        return centroidPath;
    }
}

