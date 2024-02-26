package it.unipi.hadoop.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import it.unipi.hadoop.KMeans;
import it.unipi.hadoop.mapreduce.KMeansCombiner;
import it.unipi.hadoop.mapreduce.KMeansMapAndCombine;
import it.unipi.hadoop.mapreduce.KMeansMapper;
import it.unipi.hadoop.mapreduce.KMeansReducer;
import it.unipi.hadoop.writable.Cluster;
import it.unipi.hadoop.writable.Centroid;

public class Utils {

    public static String CENTROIDS = "centroids";

    // clean file
    public static boolean cleanFile(Configuration config, Path filePath){
        try {
            FileSystem fs = FileSystem.get(config);
            if (fs.exists(filePath)) {
                fs.delete(filePath, true);
            }
        }
        catch (Exception e) {
            System.err.println(String.format("Error during the deletion of the output file %s", e));
            return false;
        }
        return true;
    }

    // get random [nCentroids] points among the [nPoints] in file [inputPath] 
    public static List<Centroid> getRandomCentroids(Configuration config, Path inputPath, int nCentroids, int nPoints, int nCoordinates){
        List<Centroid> randomCentroids = new ArrayList<>();

        Random random = new Random();
        IntStream indexStream = random.ints(0, nPoints).distinct().limit(nCentroids);
        List<Integer> indexArray = new ArrayList<>();
        indexStream.forEach(index -> indexArray.add(index));
        
        try {
            FileSystem fs = FileSystem.get(config);
            FSDataInputStream in = fs.open(inputPath);
            BufferedReader fileReader = new BufferedReader(new InputStreamReader(in));

            int index = 0;
            int centroidId = 0;
            String line = null;
            while ((line = fileReader.readLine()) != null) {
                final Cluster point = new Cluster(line);
                if (point.getCoordinates().size() != nCoordinates) {
                    System.err.println(String.format("Point with wrong number of coordinates in input file.\nExpected: %d \nReceived: %d", nCoordinates, point.getCoordinates().size()));
                    fileReader.close();
                    return null;
                }

                if (indexArray.contains(index)) {
                    final int valueId = centroidId;
                    randomCentroids.add(new Centroid(valueId, point));
                    centroidId++;
                }
                index++;
            }
            fileReader.close();

            if (index < nPoints) {
                System.err.println(String.format("Not enough points in input file.\nExpected: %d\nReceived: %d", nPoints, index));
                return null;
            }

            if (randomCentroids.size() != nCentroids) {
                System.err.println(String.format("Couldn't extract %d random points from input data", nCentroids));
                return null;
            }
        }
        catch (Exception e) {
            System.err.println(e);
            return null;
        }

        return randomCentroids;
    }

    // get centroids after iteration
    public static List<Centroid> getCentroidsFromFiles(Configuration config, Path folderPath){
        List<Centroid> newCentroids = new ArrayList<>();

        try {
            FileSystem fs = FileSystem.get(config);
            FileStatus[] fileStatus = fs.listStatus(folderPath);
            for (FileStatus status : fileStatus) {
                Path filePath = status.getPath();

                FSDataInputStream in = fs.open(filePath);
                BufferedReader fileReader = new BufferedReader(new InputStreamReader(in));

                String line = null;
                while ((line = fileReader.readLine()) != null) {
                    newCentroids.add(new Centroid(line));
                }
                fileReader.close();
                }
            
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        } 

        if (newCentroids != null) {
            newCentroids.sort(null);
        }
        return newCentroids;
    }

    // verify threshold
    public static float calculateDistanceSum(List<Centroid> previousCentroids, List<Centroid> newCentroids) {
        float distanceSum = 0;
        previousCentroids.sort(null);
        newCentroids.sort(null);

        for (int i = 0; i < previousCentroids.size(); i++) {
            Centroid next = newCentroids.get(i);
            Centroid previous = previousCentroids.get(i);
            distanceSum += Math.sqrt(previous.getPoint().getSquaredDistance(next.getPoint()));
        }

        return distanceSum;
    }

    // job configuration
    public static Job configureJob(Configuration config, 
                                    int nReducers, 
                                    List<Centroid> iterationCentroids, 
                                    int iteration, 
                                    Path inputPath, 
                                    Path outputPath, 
                                    boolean inMapperCombining) throws IOException, InterruptedException, ClassNotFoundException 
    {
        String jobName = String.format("MapReduce_n_%d", iteration);
        
        Job job = Job.getInstance(config, jobName);
        job.setJarByClass(KMeans.class);

        // Mapper/Combiner/Reducer
        if (inMapperCombining) {
            job.setMapperClass(KMeansMapAndCombine.class);
        }
        else {
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
        }
        job.setReducerClass(KMeansReducer.class);
        
        // number of reducer
        job.setNumReduceTasks(nReducers);
        
        // map output
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Cluster.class);
        
        // reduce output
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // iteration centroids
        String[] centroidStrings = new String[iterationCentroids.size()];
        for (int i = 0; i < iterationCentroids.size(); i++) {
            centroidStrings[i] = iterationCentroids.get(i).toString();
        }
        job.getConfiguration().setStrings(CENTROIDS, centroidStrings);

        // file format
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    // LOG
    public static String newLog(int nPoints, int nCoordinates, int nCentroids, int nReducers, boolean inMapperCombining) {
        String combiningMethod = (inMapperCombining ? "inMapper" : "combiner");
        String fileName = String.format("KMeans_%d_%d_%d_%d_%s_log.txt", nPoints, nCoordinates, nCentroids, nReducers, combiningMethod);
        try (FileWriter fw = new FileWriter(fileName, false);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw)) {
                out.println(String.format("NUM POINTS: %d, NUM COORDINATES: %d, NUM CENTROIDS: %d, NUM REDUCERS: %d", nPoints, nCoordinates, nCentroids, nReducers));
                out.println(String.format("COMBINING METHOD: %s", combiningMethod));
        } catch (IOException e) {
            System.err.println("Error during the write operation on the log file");
            e.printStackTrace();
            return null;
        }
        return fileName;
    }

    public static void logInfo(String logFile, String string) {
        // Use try-with-resources to automatically close the FileWriter, BufferedWriter and PrintWriter
        try (FileWriter fw = new FileWriter(logFile, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw)) {
                out.println(string);
        } catch (IOException e) {
            System.err.println("Error during the write operation on the log file");
            e.printStackTrace();
        }
    }
}
