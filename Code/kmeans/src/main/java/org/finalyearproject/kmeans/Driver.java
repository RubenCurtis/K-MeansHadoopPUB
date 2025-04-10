package main.java.org.finalyearproject.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The Driver class in the K-Means MapReduce program.
 */
public class Driver extends Configured implements Tool {

  //***IMPORTANT CONFIGURATION***
  /* Modify the following variables based on your
   * personal preferences.
   * INITIAL_CENTROIDS - the initial set of k means.
   * see https://wikipedia.org/wiki/K-means_clustering
   * PYTHONSCRIPTPATH - the location of the python script
   * to plot a graph.
   * HDFSOUTPUTPATH - the location of the Hadoop Distributed
   * File System path in which the output of the K-Means
   * algorithm is located. 
   * */
  private static final int INITIAL_CENTROIDS = 25;
  private static final String PYTHONSCRIPTPATH = 
      "/home/user/git/PROJECT/Code/kmeans/src/main/python/Plot.py";
  private static final String HDFSOUTPUTPATH = 
      "/home/user/git/PROJECT/Code/kmeans/src/main/resources/Output/final_centroids";

  /**
 * Call the Python 3 script to create the graph of results from the
 * MapReduce program.
 *
 * @return 0 on successful execution, 1 on an error.
 * @throws IOException - An error encountered by the ProcessBuilder
 */
  public static int build() throws IOException {
    ProcessBuilder build = new ProcessBuilder("python", PYTHONSCRIPTPATH, HDFSOUTPUTPATH);
    Process process = build.start();
    try {
      process.waitFor();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    int exitCode = process.exitValue();
    if (exitCode == 0) {
      return 0;
    } else {
      System.err.println("An error occurred while building the graph");
      return 1;
    }
  }
  
  /**
 * Run the MapReduce program, then plot the graph.
 *
 * @param args - The input path, the output path and the number of iterations
 * @throws Exception - Given incorrect data as input
 */
  public static void main(String[] args)
      throws Exception {
    int exitCode = ToolRunner.run(new Driver(), args);
    if (exitCode == 0) {
      exitCode = build();
    }
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("Incorrect number of arguments provided");
      return 1;
    }
    Configuration cfg = getConf();
    int iterations = Integer.parseInt(args[2]);
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    
    Path centroidPath = selectInitialCentroids(inputPath, outputPath, cfg);
    
    for (int i = 0; i < iterations; i++) {
      Job job = Job.getInstance(cfg, "kmeans");
      job.setJarByClass(Driver.class);

      FileInputFormat.addInputPath(job, inputPath);
      FileOutputFormat.setOutputPath(job, new Path(outputPath, "iteration_" + (i + 1)));

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
      
      DistributedCache.addCacheFile(centroidPath.toUri(), job.getConfiguration());
      
      if (!job.waitForCompletion(true)) {
        return 1;
      }
      centroidPath = new Path(outputPath, "iteration_" + (i + 1) + "/part-r-00000");
    }
    FileSystem fs = FileSystem.get(cfg);
    fs.rename(centroidPath, new Path(outputPath, "final_centroids"));
    fs.copyToLocalFile(new Path(outputPath, "final_centroids"), new Path(HDFSOUTPUTPATH)); 
    return 0;
  }

  private Path selectInitialCentroids(Path inputPath, Path outputPath, Configuration cfg)
      throws IOException {
    Path centroidPath = new Path(outputPath, "initial_centroids.txt");
    Random random = new Random();
    try (FileSystem fs = FileSystem.get(cfg);
        FSDataOutputStream outputStream = fs.create(centroidPath);
        FSDataInputStream inputStream = fs.open(inputPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      
      for (int i = 0; i < INITIAL_CENTROIDS; i++) {
        long fileLength = fs.getFileStatus(inputPath).getLen();
        long pos = (long) (random.nextDouble() * fileLength);
        inputStream.seek(pos);
        reader.readLine();
        String line = reader.readLine();
        
        if (line != null) {
          outputStream.writeBytes(line + "\n");
        } else {
          System.err.println("Unable to select initial centroids");
          System.exit(1);
        }
      }
    }
    return centroidPath;
  }
}
