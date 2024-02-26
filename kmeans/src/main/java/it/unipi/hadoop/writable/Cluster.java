package it.unipi.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Writable;

public class Cluster implements Writable{
    private int size;
    private List<Float> coordinates;

    public Cluster(){
        this.size = 0;
        this.coordinates = null;
    }

    public Cluster(List<Float> coordinates){
        this.size = 1;
        this.coordinates = coordinates;
    }

    public Cluster(String text){
        String[] values = text.split(";");
        List<Float> coordinates = new ArrayList<>();
        for (String value : values){
            coordinates.add(Float.parseFloat(value.trim()));
        }
        this.coordinates = coordinates;
        this.size = 1;
    }

    public List<Float> getCoordinates(){
        return this.coordinates;
    }

    public int getSize(){
        return this.size;
    }

    public void addPoint(Cluster point){
        if (point.getSize() == 0)
            return;
        if (this.size == 0) {
            this.coordinates = point.getCoordinates();
            this.size = point.getSize();
            return;
        }
        int dimensions = this.coordinates.size();
        List<Float> otherCoordinates = point.getCoordinates();
        for (int i=0 ; i<dimensions ; i++){
            this.coordinates.set(i, this.coordinates.get(i) + otherCoordinates.get(i));
        }
        this.size += point.getSize();
    }

    public Float getSquaredDistance(Cluster point){
        List<Float> otherCoordinates = point.getCoordinates();
        int dimensions = this.coordinates.size();
        float squaredDistance = 0;
        for (int i=0 ; i<dimensions ; i++){
            squaredDistance += Math.pow(this.coordinates.get(i) - otherCoordinates.get(i), 2);
        }
        return squaredDistance;
    }

    public Cluster getMean(){
        final float dim = this.size;
        List<Float> meanCoordinates = new ArrayList<>();
        for (final Float value : this.coordinates) {
            meanCoordinates.add(value / dim);
        }
        return new Cluster(meanCoordinates);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeInt(this.size);
        out.writeInt(this.coordinates.size());
        for (Float coordinate : this.coordinates){
            out.writeFloat(coordinate);
        }
    }
    
    @Override
    public void readFields(DataInput in) throws IOException{
        this.size  = in.readInt();
        int coordinatesSize = in.readInt();
        this.coordinates = new ArrayList<Float>();
        for (int i=0 ; i<coordinatesSize ; i++){
            coordinates.add(in.readFloat());
        }
    }
    
    @Override
    public String toString(){
        String coordinatesString = this.coordinates.stream().map(Object::toString).collect(Collectors.joining(";"));
        return coordinatesString;
    }
}
