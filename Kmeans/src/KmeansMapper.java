package Kmeans.src;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

  public static class KmeansMapper extends Mapper<Object, Text, IntWritable, Point> {
    private Point[] M;


/*
    @Override
    protected void setup(final Context Context) throws IOException, InterruptedException {
      super.setup(context);
      // retrieve all the centroids (global read-only) used by each Mapper
      this.M = Arrays.stream(context.getConfiguration().getStrings("M")).map(s->newPoint(s).toArray(Point[]::new));
    }
*/

    //COME VIENE PASSATO IL PUNTO? ARRAY DI DOUBLE O STRINGA??
  public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException{

    //this function finds and emits the index of the closest centroid for each point
    final Point p = new Point(); /******* VEDERE COME PASSARE PARAMETRI AL COSTRUTTORE IN BASE A COME ARRIVA IL PUNTO*/
    final Float min_dist = Float.MAX_VALUE;
    int min_index = -1;
    for(int i=0; i < M.length; i++){
      // min_dist = min_dist > M[i].distance(p) ? dist : min_distance;
      // min_index = min_dist > M[i].distance(p) ? i :index;
      final float dist = M[i].distance(p); // CONTROLLARE CENTROID == POINT
      if(min_distance>dist){
        min_distance=dist;
        min_index=i;
      }
    }
    context.write(newIntWritable(min_index), p);
  }
}
