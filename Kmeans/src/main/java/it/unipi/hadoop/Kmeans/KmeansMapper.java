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
    
    private final ArrayList<Mean> means = new ArrayList<Mean>();
    private final Point point = new Point();

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      Path centroidsPath = new Path(conf.get("centroidsFilePath"));
      FileSystem fs = FileSystem.get(conf);
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)));

      try {
        String line = br.readLine();
        while (line != null){
          double[] mean = getCoordinatesMean(line);                  
          Mean centroid = new Mean(mean, getKey(line));
          means.add(centroid);
          line = br.readLine();         
        }
      }
      
      finally {
        br.close();
      }
    }
      
  
  //this function finds and emits the index of the closest centroid for each point
  public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException{
    point.set(getCoordinatesPoint(value.toString()), 1); 
    double min_distance = Double.MAX_VALUE;
    int min_index = 0;
    int current_index=0;
    Iterator<Mean> meanIterator=means.iterator();
    double distance = 0;
    while(meanIterator.hasNext()){
      distance = point.distance(meanIterator.next());
      if(min_distance>distance){
        min_distance = distance;
        min_index = current_index;
      }
      current_index++;
    }
    context.write(means.get(min_index), point); 
  }


  private double[] getCoordinates(String[] textCoordinates, int startIndex){
    double[] doubleCoordinates = new double[textCoordinates.length - startIndex]; 
    for (int i = startIndex; i < textCoordinates.length; i++)
      doubleCoordinates[i - startIndex] = Double.parseDouble(textCoordinates[i]);   
    return doubleCoordinates;  
  }
  private double[] getCoordinatesMean(String textCoordinates){
    String[] stringCoordinates = textCoordinates.split("\t");
    return getCoordinates(stringCoordinates[1].split(" "), 0);
  }

  private double[] getCoordinatesPoint(String textCoordinates){
    String[] stringCoordinates = textCoordinates.split(" ");
    return getCoordinates(stringCoordinates, 0);
  }

  private String getKey(String line){
    return line.split("\t")[0];
  }

}
