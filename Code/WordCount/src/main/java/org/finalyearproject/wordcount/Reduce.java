package main.java.org.finalyearproject.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Performs Reduce operations in the context of a MapReduce algorithm.
 */
public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
  
  private IntWritable result = new IntWritable();

  /**
  * Takes a key value pair that has been mapped and sums the number of occurrences of the value.
  *
  * @see Map
  */
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }
}