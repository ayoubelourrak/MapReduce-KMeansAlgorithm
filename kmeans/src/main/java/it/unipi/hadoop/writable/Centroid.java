package it.unipi.hadoop.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Centroid implements WritableComparable<Centroid>{
    private final IntWritable centroidId;
    private Cluster point;

    public Centroid(int centroidId, Cluster point){
        this.centroidId = new IntWritable(centroidId);
        this.point = point;
    }

    public Centroid(String string) {
        String[] tokens = string.split("\\t");
        this.centroidId = new IntWritable(Integer.parseInt(tokens[0].trim()));
        this.point = new Cluster(tokens[1].trim());
    }

    public IntWritable getCentroidId(){
        return this.centroidId;
    }

    public Cluster getPoint(){
        return this.point;
    }

    @Override
    public int compareTo(Centroid centroid){
        return getCentroidId().compareTo(centroid.getCentroidId());
    }

    @Override
    public void write(DataOutput out) throws IOException{
        getCentroidId().write(out);
        getPoint().write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        getCentroidId().readFields(in);
        getPoint().readFields(in);
    }

    @Override
    public String toString(){
        String centroidId = getCentroidId().toString();
        String point = getPoint().toString();
        String outputString = String.format("%s\t%s", centroidId, point);
        return outputString;
    }
}
