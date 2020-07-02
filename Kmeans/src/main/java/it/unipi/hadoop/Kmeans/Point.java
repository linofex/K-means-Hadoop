package it.unipi.hadoop.Kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;

public class Point implements Writable {
  private ArrayPrimitiveWritable coordinates; // ArrayPrimitiveWritable manages int[], double[] etc
  private IntWritable pointCount; // to save the number of additions of point
  
  public Point(){
    coordinates = new ArrayPrimitiveWritable();
    pointCount = new IntWritable();
  }

  // point count starts from 1
  public Point(double[] _coordinates) {
    this.coordinates = new ArrayPrimitiveWritable(_coordinates);
    this.pointCount = new IntWritable(1);
  }

  public Point(double[] _coordinates, int sum) {
    this(_coordinates);
    if(sum > 1){
      this.pointCount.set(sum);
    } // else the other costructor sets it to 1
  }

  public Point(Point _point){
    this(_point.getCoordinates(), _point.getPointCount());
  }

  public double[] getCoordinates() {
    return (double[]) coordinates.get();
  }
  
  public int getPointCount() {
    return (int) pointCount.get();
  }


  public void set(double[] point, int sum){
    this.coordinates.set(point);
    this.pointCount.set(sum);

  }
  

  
  public void add(double[] point, int sum){
    double[] thisPoint = this.getCoordinates(); //ref
    if(thisPoint.length != point.length){
      //manage it
    }
    System.out.println("this coord1: " + this.toString());
    for (int i = 0; i < point.length; i++) {
      thisPoint[i] += point[i];
    }
    this.pointCount.set(this.getPointCount() + sum);

  }

  public void add(Point point){
    double[] argPoint = point.getCoordinates();
    this.add(argPoint, point.getPointCount());
  }

  public double distance(Point that){
    double[] thisPoint = this.getCoordinates();
    double[] thatPoint = that.getCoordinates();
    double sum = 0;
    //check same length
    if(thisPoint.length != thatPoint.length){
      //manage it
    }

    for (int i = 0; i < thatPoint.length; i++) {
      sum += Math.pow(thatPoint[i] - thisPoint[i], 2);
    }
    return Math.sqrt(sum);
    

  }

  // Writable interface implementation

  @Override
  public void write(DataOutput out) throws IOException {
    this.coordinates.write(out);
    this.pointCount.write(out);

  }
  @Override
  public void readFields(DataInput in) throws IOException {
    this.coordinates.readFields(in);
    this.pointCount.readFields(in);

  }

  @Override
  public String toString(){
      double [] coords=this.getCoordinates();
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i<coords.length;i++) {
          sb.append(coords[i]);
          if (i < coords.length - 1) {
              sb.append(" ");
          }
      }
      return sb.toString();
  }

}