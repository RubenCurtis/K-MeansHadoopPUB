package main.java.org.finalyearproject.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Used by Hadoop to launch a MapReduce job to count word
 * occurrences.
 */
public class Driver {

  /**
  * Creates a new configuration, job and sets the specific 
  * classes to be used for the job.
  *
  * @param args - Main class
  * @throws Exception - if Hadoop job fails
  * @see Map
  * @see Reduce
  */
  public static void main(String[] args)
      throws Exception {
    Configuration cfg = new Configuration();
    Job job = Job.getInstance(cfg, "WordCount");
    job.setJarByClass(Driver.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setNumReduceTasks(4); //CHANGE ME TO 0.95 * NODES
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
