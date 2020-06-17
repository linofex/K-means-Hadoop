package it.unipi.hadoop.Kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.ArrayList;
import java.util.Iterator;



  public class KmeansMapper extends Mapper<Object, Text, Mean, Point> {
    
    private ArrayList<Mean> means = new ArrayList<>();
    private final Point point = new Point();

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      Path centroidsPath = new Path(conf.get("centroidsFilePath"));
      FileSystem fs = FileSystem.get(conf);
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)));
      // int centroid_index = 0;
      try {
        String line = br.readLine();
        while (line != null){                     
          Mean centroid = new Mean(getCoordinates(line), getKey(line));
          means.add(centroid);
          line = br.readLine();
        }
      }
      catch (Exception e) {
    
      }
      finally {
        br.close();
      }
      //bisogna vedere se i centroidi sono letti all'inizializzazione o no
      // se inizializzazione, file unico, altrimenti più file
    }
      

  public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException{
    //this function finds and emits the index of the closest centroid for each point
    point.set(getCoordinates(value.toString()), 1); 
    double min_distance = Double.MAX_VALUE;
    int min_index = 0;
    int current_index=0;
    Iterator<Mean> meanIterator=means.iterator();
    while(meanIterator.hasNext()){
      final double distance = point.distance(meanIterator.next());
      if(min_distance>distance){
        min_distance = distance;
        min_index = current_index;
      }
      current_index++;
    }

    context.write(means.get(min_index), point); 
  }

  private double[] getCoordinates(String textCoordinates){
    String[] stringCoordinates = textCoordinates.split(" ");
    double[] doubleCoordinates = new double[stringCoordinates.length -1]; 
    for (int i = 1; i < doubleCoordinates.length; i++)
      doubleCoordinates[i] = Double.parseDouble(stringCoordinates[i]);   
    return doubleCoordinates;
  }

  private String getKey(String line){
    return line.split(" ")[0];
  }

}
