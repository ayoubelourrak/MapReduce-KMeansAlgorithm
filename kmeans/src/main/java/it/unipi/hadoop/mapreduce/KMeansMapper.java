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
import java.util.List;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Cluster>{

    private List<Centroid> centroids;

    @Override
    public void setup(Context context){
        Configuration conf = context.getConfiguration();
        String[] centroidStrings = conf.getStrings(Utils.CENTROIDS);
        
        centroids = new ArrayList<>();
        for (String s : centroidStrings) {
            centroids.add(new Centroid(s));
        }
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

        context.write(centroidId, point);
    }
}
