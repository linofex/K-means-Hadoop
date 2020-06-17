package it.unipi.hadoop.Kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import java.util.ArrayList;
import java.util.Iterator;



public class KmeansReducer extends Reducer<Mean, Point, Text, NullWritable> {
    private double threshold;
    private ArrayList<Mean> centroidsList;
    private Text centroidOutput = new Text();

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        threshold = conf.getDouble("threshold", 0.);
        centroidsList = new ArrayList<Mean>();
    }

    @Override
    public void reduce(Mean key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
        Iterator<Point> pointIterator = points.iterator();
        Point value = pointIterator.next();
        while(pointIterator.hasNext())
            value.add(pointIterator.next());  
        double[] coordinates = value.getCoordinates();
        int pointCounter = value.getPointCount();
        for(int index=0;index<coordinates.length;index++)
            coordinates[index]/=pointCounter;
        Mean newCentroid=new Mean(new Point(coordinates),key.getId());
        if(key.distance(newCentroid) >= threshold) 
            context.getCounter(CentroidCounter.NUMBER_OF_UNCONVERGED).increment(1);
        centroidsList.add(newCentroid);

        centroidOutput.set(newCentroid.toString());
        context.write(centroidOutput, NullWritable.get());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path centroidsPath = new Path(conf.get("centroidsFilePath"));
        FileSystem fs = FileSystem.get(conf);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(centroidsPath, true)));
        Iterator<Mean> meansIterator = centroidsList.iterator();
        meansIterator.forEachRemaining(line -> {
            try {
		    	bw.write(line.getId() + " " +  line.toString());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }});

    }



}