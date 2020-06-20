package it.unipi.hadoop.Kmeans;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class KmeansCombiner extends Reducer<Mean, Point, Mean , Point> { // per ora utilizzo Point

    @Override
    protected void reduce(Mean key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
        // devo inizializzare il Point value
        // System.out.println("Combiner: "+ key.toString());
        // Iterator<Point> pointsIterator=points.iterator();
        // Point value= pointsIterator.next();
        // pointsIterator.forEachRemaining(point -> {value.add(point);
        //     System.out.println("int: mean: " + key.getId() + " count: " + value.getPointCount() );
        // });
        // while(pointsIterator.hasNext()){
        //     Point p = pointsIterator.next();
        //     System.out.println("value: " + value.toString());
        //     System.out.println("p: " + p.toString());
        //     value.add(p);
        // }
        Point result = null;
        for (Point point : points) {
            if (result == null) {
                result = new Point(point);
            } 
            else {
                result.add(point);
            }
        }

        System.out.println("mean: " + key.getId() + " count: " + result.getPointCount() );
        context.write(key, result);
    }
}