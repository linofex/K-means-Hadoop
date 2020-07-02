package it.unipi.hadoop.Kmeans;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class KmeansCombiner extends Reducer<Mean, Point, Mean , Point> { 
    @Override
    protected void reduce(Mean key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
        Iterator<Point> pointsIterator=points.iterator();
        Point pointsSum= new Point(pointsIterator.next());
        pointsIterator.forEachRemaining(point -> {pointsSum.add(point);});    
        context.write(key, pointsSum);
    }
}