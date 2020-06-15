package Kmeans.src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
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
        meansIterator.forEachRemaining(line -> {bw.write(line.toString());});

    }



}