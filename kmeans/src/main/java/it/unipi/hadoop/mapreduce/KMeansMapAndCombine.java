package it.unipi.hadoop.mapreduce;

import it.unipi.hadoop.utils.Utils;
import it.unipi.hadoop.writable.Cluster;
import it.unipi.hadoop.writable.Centroid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KMeansMapAndCombine extends Mapper<LongWritable, Text, IntWritable, Cluster>{

    private List<Centroid> centroids;
    private Map<IntWritable,Cluster> outputMap;

    @Override
    public void setup(Context context){
        Configuration conf = context.getConfiguration();
        String[] centroidStrings = conf.getStrings(Utils.CENTROIDS);
        
        centroids = new ArrayList<>();
        for (String s : centroidStrings) {
            centroids.add(new Centroid(s));
        }

        outputMap = new HashMap<>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        Cluster point = new Cluster(value.toString());
        IntWritable centroidId = null;
        Float distanceMinimal = Float.MAX_VALUE;

        for (Centroid centroid : this.centroids) {
            final IntWritable tmp = centroid.getCentroidId();
            Float squaredDistance = point.getSquaredDistance(centroid.getPoint());
            if (squaredDistance < distanceMinimal){
                distanceMinimal = squaredDistance;
                centroidId = tmp;
            }
        }

        if (outputMap.containsKey(centroidId)){
            Cluster oldPoint = outputMap.get(centroidId);
            oldPoint.addPoint(point);
            outputMap.replace(centroidId, oldPoint);
        } else {
            outputMap.put(centroidId, point);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<IntWritable,Cluster> centroid : outputMap.entrySet()) {
            context.write(centroid.getKey(), centroid.getValue());
        }   
    }
}
