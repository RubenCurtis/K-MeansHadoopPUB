package main.java.org.finalyearproject.frequencydistribution;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  Take a mapped list of key,value pairs and perform a reduce operation to
 *  collate the mapped data.
 */
public class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

  private LongWritable result = new LongWritable();

  /**
   * Reduce the given mapped values into a sum.
   *
   * @param key - The key in the key,value pair in map
   * @param values - The values in the key,value pair in map
   * @param context - The context of a mapreduce job, used by hadoop job
   * @throws IOException - If the mapreduce job has been interrupted
   * @throws InterruptedException - If an invalid value is discovered
   */
  public void reduce(Text key, Iterable<LongWritable> values, Context context) 
      throws IOException, InterruptedException {
    int sum = 0;
    for (LongWritable value : values) {
      sum += value.get();
    }
    result.set(sum);
    context.write(key, result);
  }
}
