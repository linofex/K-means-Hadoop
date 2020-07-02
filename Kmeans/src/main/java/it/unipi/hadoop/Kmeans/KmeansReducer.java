package it.unipi.hadoop.Kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import java.util.Iterator;

public class KmeansReducer extends Reducer<Mean, Point, Text, Text> {
    private double threshold;
    private final Text centroidOutputId = new Text();
    private final Text centroidOutput = new Text();
    

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        threshold = conf.getDouble("threshold", 0.);
    }

    @Override
    public void reduce(Mean key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
        Iterator<Point> pointsIterator=points.iterator();
        Point pointsSum= new Point(pointsIterator.next());
        pointsIterator.forEachRemaining(point -> {pointsSum.add(point);});
        double[] coordinates = pointsSum.getCoordinates();
        int pointCounter = pointsSum.getPointCount();
        for(int index=0;index<coordinates.length;index++)
            coordinates[index]/=pointCounter;
        Mean newCentroid = new Mean(coordinates,key.getId());
        if(key.distance(newCentroid) >= threshold) 
            context.getCounter(CentroidCounter.NUMBER_OF_UNCONVERGED).increment(1);
        
        centroidOutputId.set(newCentroid.getId());
        centroidOutput.set(newCentroid.toString());
        context.write(centroidOutputId, centroidOutput);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        
    }
}