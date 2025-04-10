package main.java.org.finalyearproject.distributedgrep;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reduce job for a regular expression.
 */
public class Reduce extends Reducer<Text, Text, Text, Text> {

  /**
  * Handles the reduce section of the MapReduce algorithm.
  */
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    for (Text value : values) {
      context.write(key, value);
    }
  }
}
