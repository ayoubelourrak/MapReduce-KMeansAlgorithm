package it.unipi.hadoop;

import it.unipi.hadoop.utils.Utils;
import it.unipi.hadoop.writable.Centroid;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class KMeans {

    public static void main (String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Arguments should be: <inputFilePath> <outputDirPath> <configurationFilePath>");
            System.exit(1);
        }
        // get configuration
        Configuration config = new Configuration();
        config.addResource(new Path(args[2]));

        // set parameters
        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);
        final int nCentroids = config.getInt("nCentroids", 3);
        final int nReducers = config.getInt("nReducers", 1);
        final int nPoints = config.getInt("nPoints", 100);
        final int nCoordinates = config.getInt("nCoordinates", 2);
        final Float threshold = config.getFloat("threshold", 0.0001F);
        final int maxIterations = config.getInt("maxIterations", 20);
        final long maxExecutionTime = config.getLong("maxExecutionTime", 300);
        final boolean inMapperCombining = config.getBoolean("inMapperCombining", false);
    

        long initTime = System.currentTimeMillis();
        double executionTime = 0.0; // in seconds
        double meanTime = 0.0;
        float distanceSum = 0;

        if (maxIterations < 1 || nCentroids < 2) {
            System.err.println("Invalid Configuration");
            System.exit(1);
        }
        
        // set random values as centroids
        List<Centroid> iterationCentroids = Utils.getRandomCentroids(config, inputPath, nCentroids, nPoints, nCoordinates);
        if (iterationCentroids == null) {
            System.err.println("Failed random centroid picking");
            System.exit(1);
        }

        // create log file
        String logFile = Utils.newLog(nPoints, nCoordinates, nCentroids, nReducers, inMapperCombining);
        String firstLogString = "";
        firstLogString += "Randomly selected centroids:\n";
        for (Centroid centroid : iterationCentroids) {
            firstLogString += centroid.toString() + "\n";
        }
        Utils.logInfo(logFile, firstLogString);

        //////////////////// ITERATIONS
        int iteration = 0;
        for (; iteration < maxIterations; iteration++){

            // clean output file
            if (!Utils.cleanFile(config, outputPath)) {
                System.exit(1);
            }

            long preTimeStamp = System.currentTimeMillis();
            // configure jobs
            try {
                Job job = Utils.configureJob(config, nReducers, iterationCentroids, iteration, inputPath, outputPath, inMapperCombining);
                if (job == null) {
                    System.err.println("Error in Job configuration");
					System.exit(1);
                }
                if (!job.waitForCompletion(true)) {
                    System.err.println("Error during Job execution");
					System.exit(1);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            long postTimestamp = System.currentTimeMillis();
            double iterationTime = (double) (postTimestamp - preTimeStamp) / 1000.0; // in seconds
            meanTime += iterationTime;
            executionTime = (double) (System.currentTimeMillis() - initTime) / 1000.0;

            // read new centroids
            List<Centroid> newCentroids = Utils.getCentroidsFromFiles(config, outputPath);

            // update iteration centroids and distance from previous centroids
            distanceSum = Utils.calculateDistanceSum(iterationCentroids, newCentroids);
            iterationCentroids = newCentroids;
            
            // log info
            String logString = "";
            logString += "-----------------------------------------------------------------\n";
            logString += String.format("K-MEANS Iteration: %d, Iteration time: %s, Current distance: %s\n", iteration, iterationTime, distanceSum);
            logString += "Current centroids:\n";
            for (Centroid centroid : iterationCentroids) {
                logString += centroid.toString() + "\n";
            }
            logString += "-----------------------------------------------------------------\n";
            Utils.logInfo(logFile, logString);

            // verify convergence
            if (distanceSum < threshold) {
                Utils.logInfo(logFile, "-- REACHED THRESHOLD");
                break;
            }

            // check execution time
            if (executionTime >= maxExecutionTime) {
                Utils.logInfo(logFile, "-- EXECUTION TIMEOUT");
                break;
            }

        }
        
        if (iteration >= maxIterations) {
            Utils.logInfo(logFile, "-- REACHED MAXIMUM NUMBER OF ITERATIONS");
        }
        // get average execution time
        meanTime /= (double) iteration;

        Utils.logInfo(logFile, "----------------------- END OF ITERATIONS -----------------------");
        Utils.logInfo(logFile, String.format("Total Execution Time: %s", executionTime));
        Utils.logInfo(logFile, String.format("Mean Iteration Time: %s", meanTime));
        Utils.logInfo(logFile, String.format("Final Distance: %s", distanceSum));
        Utils.logInfo(logFile, "FINAL CENTROIDS:");
        for (Centroid centroid : iterationCentroids) {
            Utils.logInfo(logFile, centroid.toString());
        }
    }
}
