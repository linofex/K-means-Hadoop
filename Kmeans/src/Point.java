package Kmeans.src;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.commons.math3.ml.distance;

import java.util.List;

import org.apache.hadoop.io.ArrayPrimitiveWritable;

public class Point implements Writable {
  private ArrayPrimitiveWritable coordinates; // ArrayPrimitiveWritable manages int[], double[] etc
  private IntWritable pointCount; // to save the number of additions of point
  
  // public Point(){
  //   vector = new ArrayPrimitiveWritable();
  //   number = new IntWritable(0);
  // }

  // point count starts from 1
  public Point(double[] _coordinates) {
    this.coordinates = new ArrayPrimitiveWritable(_coordinates);
    this.pointCount.set(1);
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
    return coordinates.get();
  }
  
  public int getPointCount() {
    return pointCount.get();
  }

  public void add(double[] point){
    double[] thisPoint = this.getCoordinates();
    if(thisPoint.length != point.length){
      //manage it
    }
    for (int i = 0; i < point.length; i++) {
      thisPoint[i] += point[i];
    }
    this.coordinates.set(thisPoint);
    this.pointCount.set(this.getPointCount() + 1);

  }

  public void add(double[] point, int sum){
    this.add(point);
    this.pointCount.set(this.getPointCount() + sum - 1); //-1 since previous add adds 1. 

  }

  public void add(Point point){ //excemption
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
    // euclidian distance  A and B = Square root( summing up (A[i] - B[i])^2)

    //or EuclideanDistance.

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


}