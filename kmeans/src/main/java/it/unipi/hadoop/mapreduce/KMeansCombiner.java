package it.unipi.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.hadoop.writable.Cluster;

public class KMeansCombiner extends Reducer<IntWritable, Cluster, IntWritable, Cluster>{
    
    @Override
    public void reduce(IntWritable meanId, Iterable<Cluster> pointsList, Context context) throws IOException, InterruptedException{
        Cluster cluster = new Cluster();

        for (final Cluster point : pointsList) {
            cluster.addPoint(point);
        }

        context.write(meanId, cluster);
    }
}
