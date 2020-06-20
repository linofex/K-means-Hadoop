package it.unipi.hadoop.Kmeans;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class KmeansCombiner extends Reducer<Mean, Point, Mean , Point> { // per ora utilizzo Point

    @Override
    protected void reduce(Mean key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
        
        System.out.println("DC: "+ key.toString());
        Iterator<Point> pointsIterator=points.iterator();
        Point value= pointsIterator.next();
        pointsIterator.forEachRemaining(point -> {value.add(point)});
        /*while(pointsIterator.hasNext())
            value.add(pointsIterator.next());
        context.write(key, value);*/
    }
}