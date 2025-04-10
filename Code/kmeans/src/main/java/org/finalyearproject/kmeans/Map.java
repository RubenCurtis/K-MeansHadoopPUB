package main.java.org.finalyearproject.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * The Map in the MapReduce program, simulating the assignment step
 * of the K-Means algorithm, using Forgy centroid initialisation.
 */
public class Map extends Mapper<LongWritable, Text, IntWritable, Text> {

  private Path centroidsPath = new Path("/output/initial_centroids.txt");
  private List<double[]> centroids = new ArrayList<>(); 
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(context.getConfiguration());
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split(","); //MAKE IT A .csv
        double[] centroid = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
          centroid[i] = tokens[i].isEmpty() ? 0.0 : Double.parseDouble(tokens[i]);
        }
        centroids.add(centroid);
      }
    }
  }
  
  
  /**
 * Take a value of N dimensions, Map it to its nearest centroid.
 *
 * @param key - the identifier of the data
 * @param value - the data itself
 * @param context - used by MapReduce to provide itself context
 * @throws InterruptedException - given the MapReduce program has been interrupted
 * @throws IOException 
 *
 */
  protected void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
    String[] xy = value.toString().trim().split(",");
    double[] dataPoint = new double[xy.length];
    for (int i = 0; i < xy.length; i++) {
      dataPoint[i] = xy[i].isEmpty() ? 0.0 : Double.parseDouble(xy[i]);
    }
    
    double minDistance = Double.MAX_VALUE;
    int nearestCentroidIndex = -1;
    
    for (int i = 0; i < centroids.size(); i++) {
      double distance = calculateDistance(dataPoint, centroids.get(i));
      if (distance < minDistance) {
        minDistance = distance;
        nearestCentroidIndex = i;
      }
    }
    if (nearestCentroidIndex != -1) {
      context.write(new IntWritable(nearestCentroidIndex), new Text(String.valueOf(value)));
    }
  }


  /**
 * Find the squared euclidean distance between two points.
 * Used in calculating the nearest centroid.
 *
 * @param point1 - the first point to be compared
 * @param point2 - the second point to be compared
 * @return - the squared euclidean distance between the two points
 */
  private double calculateDistance(double[] point1, double[] point2) {
    if (point1.length != point2.length) {
      throw new IllegalArgumentException("Error in the dataset provided");
    }
    
    double sum = 0.0;
    for (int i = 0; i < point1.length; i++) {
      double difference = point2[i] - point1[i];
      sum += difference * difference;
    }
    return Math.sqrt(sum);
  }
}
