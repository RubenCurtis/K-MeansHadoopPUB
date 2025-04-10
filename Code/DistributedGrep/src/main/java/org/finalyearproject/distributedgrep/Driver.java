package main.java.org.finalyearproject.distributedgrep;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Will find matching regular expressions given a user input
 * regular expression using the MapReduce algorithm on a 
 * Hadoop cluster.
 *
 * @see Tool
 */
public class Driver extends Configured implements Tool {

  /**
  * Calls the MapReduce job, exits with its given exitCode.
  *
  * @param args - Main Arguments
  * @throws Exception - If the Hadoop job fails
  * @see run
  */
  public static void main(String[] args)
      throws Exception {
    int exitCode = ToolRunner.run(new Driver(), args);
    System.exit(exitCode);
  }

  /**
  * Take a user input, set the configuration of the MapReduce Job, wait for
  * the MapReduce job to complete and give an exit code.
  *
  * @return exitCode for the MapReduce job
  * @throws Exception - If invalid input is given to the BufferedReader
  */
  @Override
  public int run(String[] args)
      throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("Please enter a valid regular expression to search: ");
    String searchQuery = (br.readLine());
    Configuration cfg = new Configuration();
    cfg.set("SearchQuery.Regex", searchQuery);
    Job regex = Job.getInstance(cfg, "RegexSearch");
    regex.setJarByClass(this.getClass());
    regex.setMapperClass(RegexMapper.class);
    regex.setReducerClass(Reduce.class);
    regex.setMapOutputKeyClass(Text.class);
    regex.setMapOutputValueClass(Text.class);
    
    regex.setNumReduceTasks(4); //CHANGE ME TO 0.95 * NODES

    FileInputFormat.setInputPaths(regex, new Path(args[0]));
    FileOutputFormat.setOutputPath(regex, new Path(args[1]));
    return regex.waitForCompletion(true) == true ? 0 : 1;
  }
}
