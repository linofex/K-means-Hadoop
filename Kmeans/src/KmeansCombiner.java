package Kmeans.src;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class KmeansCombiner extends Reducer<Mean, Point, Mean , Point> { // per ora utilizzo Point

    @Override
    protected void reduce(Mean key, Iterable<Point> points, Context context) throws IOException, InterruptedException {
        // devo inizializzare il Point value
        Itarator<Point> pointsIterator=points.itarator();
        Point value= (Point) points.next();
        while(points.hasNext())
            value.add((Point)points.next());
        context.write(key, value);
    }
}