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

  public static class KmeansMapper extends Mapper<Object, Text, Mean, Point> {
    
    private Iterator<Mean> meansIterator;


    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {

      ArrayList<Mean> means  = new ArrayList<>(); 
      Configuration conf = context.getConfiguration();
      Path centroidsPath = new Path(conf.get("centroidsFilePath"));
      FileSystem fs = FileSystem.get(conf);
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)));

      try {
        String line = br.readLine();
        while (line != null){          
          String[] stringCoordinates = line.split(" ");
          double[] doubleCoordinates = new double[stringCoordinates.length]; 
        for (int i = 0; i < doubleCoordinates.length; i++)
          doubleCoordinates[i] = Double.parseDouble(stringCoordinates[i]);           
        //costruisci il Point
        Mean centroid = new Mean(new Point(doubleCoordinates), centroid_index);
        means.add(centroid);
        // be sure to read the next line otherwise you'll get an infinite loop
        line = br.readLine();
        }
      } 
      finally {
        meansIterator=means.iterator();
        br.close();
      }
      //bisogna vedere se i centroidi sono letti all'inizializzazione o no
    // se inizializzazione, file unico, altrimenti più file
    }
      

    //COME VIENE PASSATO IL PUNTO? ARRAY DI DOUBLE O STRINGA??
  public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException{

    //this function finds and emits the index of the closest centroid for each point
    final Point p = new Point(value.toString()); //RIVEDI
    final Float min_distance = Float.MAX_VALUE;
    int min_index = -1;
    for(int current_index=0; current_index < means.length; current_index++){
      final float distance = means[i].distance(p);
      
      // min_distance = min_distance > means[current_index].distance(p) ? distance : min_distance;
      // min_index = min_distance > means[current_index].distance(p) ? current_index : min_index;
      
      if(min_distance>distance){
        min_distance = distance;
        min_index = current_index;
      }
    }
    context.write(newIntWritable(min_index), p); //vedere cosa passi
  }
}
