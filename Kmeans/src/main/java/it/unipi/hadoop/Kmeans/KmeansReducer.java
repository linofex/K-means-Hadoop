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

public class KmeansReducer extends Reducer<Mean, Point, Text, Text> {
    private double threshold;
    private ArrayList<Mean> centroidsList;
    private Text centroidOutputId = new Text();
    private Text centroidOutput = new Text();
    

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        threshold = conf.getDouble("threshold", 0.);
        centroidsList = new ArrayList<Mean>();
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
        centroidsList.add(newCentroid);
        
        centroidOutputId.set(newCentroid.getId());
        centroidOutput.set(newCentroid.toString());
        context.write(centroidOutputId, centroidOutput);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    //     Configuration conf = context.getConfiguration();
    //     Path centroidsPath = new Path(conf.get("centroidsFilePath"));
    //     FileSystem fs = FileSystem.get(conf);
        
    //     OutputStreamWriter os = (fs.exists(centroidsPath))? new OutputStreamWriter(fs.append(centroidsPath)) 
    //                                                         : new OutputStreamWriter(fs.create(centroidsPath, true));
    //     BufferedWriter bw =  new BufferedWriter(os);

    //     Iterator<Mean> meansIterator = centroidsList.iterator();
    //     System.out.println("cleanup");
    //     meansIterator.forEachRemaining(line -> {
    //         try {
    //             bw.write(line.getId() + "\t" + line.toString() + "\n") ;
    //         } catch (IOException e) {
    //             e.printStackTrace();
    //         }});
    //     bw.close();
    }
}