package main.java.org.finalyearproject.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reduce method in the MapReduce algorithm. Emulating the update
 * step in the K-Means algorithm.
 */
public class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {

  protected void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    List<double[]> dataPoints = new ArrayList<>();

    for (Text value : values) {
      String[] tokens = value.toString().trim().split(",");
      double[] dataPoint = new double[tokens.length];
      for (int i = 0; i < tokens.length; i++) {
        dataPoint[i] = tokens[i].isEmpty() ? 0.0 : Double.parseDouble(tokens[i]);
      }
      dataPoints.add(dataPoint);
    }
    double[] newCentroid = computeNewCentroid(dataPoints);
    context.write(key, new Text(arrayToString(newCentroid)));
  }
  
  /**
 * Convert a double[] to String, so it is compatible with the 
 * Hadoop Text type constructor.
 *
 * @param array - the array to be converted to String
 * @return - the String format of the array passed as a parameter.
 */
  private String arrayToString(double[] array) {
    StringBuilder sb = new StringBuilder();
    for (double value : array) {
      sb.append(value).append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }


  /**
  * Find the new centroid given all of the datapoints assigned to the current one.
  *
  * @param dataPoints - all of the datapoints assigned to the centroid
  *     being recalculated
  * @return - The new centroid to be used in the next iteration or to be a
  *     finished result
  */
  private double[] computeNewCentroid(List<double[]> dataPoints) {
    int dimensions = dataPoints.get(0).length;
    double[] centroid = new double[dimensions];
    for (int i = 0; i < dimensions; i++) {
      double sum = 0.0;
      for (double[] dataPoint : dataPoints) {
        sum += dataPoint[i];
      }
      centroid[i] = sum / dataPoints.size();
    }
    return centroid;
  }
}
