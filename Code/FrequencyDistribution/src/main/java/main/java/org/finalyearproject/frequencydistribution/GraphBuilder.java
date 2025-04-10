package main.java.org.finalyearproject.frequencydistribution;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * The driver class for a mapreduce job, along with calling the required python
 * script in order to create a visual graph of mapreduced data.
 *
 * @see Tool
 */
public class GraphBuilder extends Configured implements Tool {
  //Configure these based on the setup of your hadoop cluster and local machine
  private static String hdfsPath = "hdfs://192.168.56.101:9000";
  private static String remoteFilePath = "/output/part-r-00000";
  private static String localFilePath = 
      "/home/user/git/PROJECT/Code/FrequencyDistribution/src/main/python/plot.txt";
  private static String pythonScriptPath = 
      "/home/user/git/PROJECT/Code/FrequencyDistribution/src/main/python/Plot.py";

  /**
   * Call a mapreduce job to create a collated set of a large group of numbers 
   * followed by calling a python script to build a frequency distribution graph using matplotlib.
   *
   * @param args - Main arguments
   * @throws Exception - If the mapreduce job or building the graph fails
  */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new GraphBuilder(), args);
    if (exitCode == 0) {
      exitCode = build();
    }
    if (exitCode == 1) {
      throw new Exception("Error encountered while attempting to build the graph");
    }
  }


  /**
   * Set the correct hdfs, take a file as an argument, copy it from the remote hdfs, create a 
   * matplotlib graph using the mapreduce output.
   *
   * @return The exit code of the program
   * @throws IOException - Given an invalid file name and/or location
   */
  public static int build() throws IOException {

    int exitCode;
    Configuration cfg = new Configuration();
    cfg.set("fs.defaultFS", hdfsPath);
    try {
      FileSystem fs = FileSystem.get(cfg);
      Path sourcePath = new Path(remoteFilePath);
      Path destination = new Path(localFilePath);
      FileUtil.copy(fs, sourcePath, new File(destination.toString()), false, cfg);
      
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    try {
      ProcessBuilder build = new ProcessBuilder("python", pythonScriptPath);
      Process process = build.start();
      exitCode = 0;
      BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
      BufferedReader error = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      String line;
      while ((line = input.readLine()) != null) {
        System.out.println(line);
      }
      String lines;
      while ((lines = error.readLine()) != null) {
        System.out.println(lines);
      }
      return exitCode;

    } catch (IOException e) {
      exitCode = 1;
      e.printStackTrace();
      return exitCode;
    }
  }

  @Override
public int run(String[] args) throws Exception {
    Configuration cfg = new Configuration();
    Job job = Job.getInstance(cfg, "FrequencyDistribution");
    job.setJarByClass(this.getClass());
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    
    job.setNumReduceTasks(1);
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) == true ? 0 : 1;
  }
}
