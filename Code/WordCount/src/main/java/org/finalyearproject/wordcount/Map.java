package main.java.org.finalyearproject.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Performs Map operations in the context of a MapReduce algorithm.
 */
public class Map extends Mapper<Object, Text, Text, IntWritable> {

  private static final IntWritable one = new IntWritable(1);
  private Text word = new Text();

  /**
  * Map a single word to a generated key.
  *
  * @see Reduce
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