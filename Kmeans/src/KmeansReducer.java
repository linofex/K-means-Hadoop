package Kmeans.src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;



public class KmeansReducer extends Reducer<Mean, Point, Text, NullWritable> {
    private double threshold;
    private Text centroidOutput = new Text();

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        threshold = conf.getDouble("threshold", 0.);
    }

    @Override
    public void reduce(Mean key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
        Point value = (Point) points.next();
        while(points.hasNext())
            value.add((Point)points.next());  
        double[] coordinates = value.getCoordinates();
        int pointCounter = value.getPointCount();
        for(index=0;index<coordinates.length;index++)
            coordinates[index]/=pointCounter;
        Mean newCentroid=new Mean(new Point(coordinates),key.getId());
        if(key.distance(newCentroid) >= threshold) 
            // incrementa il contatore
        centroidOutput.set(newCentroid.toString());
        context.write(centroidOutput, NullWritable.get());
    }



}