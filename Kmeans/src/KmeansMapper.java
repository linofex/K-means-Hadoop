package Kmeans.src;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

  public static class KmeansMapper extends Mapper<Object, Text, IntWritable, Point> {
    private Point[] means;


/*
    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
      super.setup(context);
      Configuration conf = context.getConfiguration();

      // retrieve all the centroids (global read-only) used by each Mapper
      this.means = Arrays.stream(context.getConfiguration().getStrings("means")).map(s->newPoint(s).toArray(Point[]::new));
    }
*/

    //COME VIENE PASSATO IL PUNTO? ARRAY DI DOUBLE O STRINGA??
  public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException{

    //this function finds and emits the index of the closest centroid for each point
    final Point p = new Point(value.toString()); /******* VEDERE COME PASSARE PARAMETRI AL COSTRUTTORE IN BASE A COME ARRIVA IL PUNTO*/
    final Float min_distance = Float.MAX_VALUE;
    int min_index = -1;
    for(int current_index=0; current_index < means.length; current_index++){
      // min_distance = min_distance > means[current_index].distance(p) ? distance : min_distance;
      // min_index = min_distance > means[current_index].distance(p) ? current_index : min_index;
      final float distance = means[i].distance(p); // CONTROLLARE CENTROID == POINT
      if(min_distance>distance){
        min_distance = distance;
        min_index = current_index;
      }
    }
    context.write(newIntWritable(min_index), p);
  }
}
