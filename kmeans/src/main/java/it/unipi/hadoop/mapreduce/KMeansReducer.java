package it.unipi.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.hadoop.writable.Cluster;

public class KMeansReducer extends Reducer<IntWritable, Cluster, IntWritable, Text>{
    
    @Override
    public void reduce(IntWritable meanId, Iterable<Cluster> pointsList, Context context) throws IOException, InterruptedException{
        Cluster cluster = new Cluster();

        for (final Cluster point : pointsList) {
            cluster.addPoint(point);
        }

        Text centroidText = new Text(cluster.getMean().toString());

        context.write(meanId, centroidText);
    }
}
