package main.java.org.finalyearproject.frequencydistribution;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to sum all occurrences and value of a numerical dataset.
 */
public class Map extends Mapper<Object, Text, Text, LongWritable> {

  private static final LongWritable one = new LongWritable(1);
  private Text word = new Text();

  /**
   * Map a list of given values to a numerical category, summing the values given.
   *
   * @param key - The key in the key,value pair in map
   * @param value - The value in the key,value pair in map
   * @param context - The context of a mapreduce job, used by hadoop job
   * @throws InterruptedException - If the mapreduce job has been interrupted
   * @throws IOException - If an invalid value is discovered
   */
  public void map(Object key, Text value, Context context) 
      throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      context.write(word, one);
    }
  }
}
