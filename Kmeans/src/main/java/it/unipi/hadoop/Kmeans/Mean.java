package it.unipi.hadoop.Kmeans;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class Mean extends Point implements Writable, Comparable<Mean>{
  private Text id; // Text should save byte wrt IntWritable

  public Mean(Point coordinates, String id){
    super(coordinates);
    this.id.set(id);
  }

  public Mean(double[] coordinates, String id){
    super(coordinates);
    this.id.set(id);
  }

  public String getId(){
    return this.id.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
  super.write(out);
  this.id.write(out);

  }
  
  @Override
  public void set(double[] point, int sum, String label){
    super.set(point, sum);
    id.set(label);

  }
  

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.id.readFields(in);

  }

  @Override
  public int compareTo(Mean that){
    return (this.id.compareTo(that.getId())); // Text compare works as ASCII
  }

}