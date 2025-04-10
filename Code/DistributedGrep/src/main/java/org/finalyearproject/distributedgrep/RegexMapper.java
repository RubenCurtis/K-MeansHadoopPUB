package main.java.org.finalyearproject.distributedgrep;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This is where the comparison for the regular expression is done.
 * Where the search query is compared to the individual line of a file.
 * File name and line number is returned (with a counter)
 */
public class RegexMapper extends Mapper<LongWritable, Text, Text, Text> {

  private Text outputKey = new Text();
  private Text outputValue = new Text();

  private Pattern pattern;

  /**
  * Setter for pattern, takes the input from a users BufferredReader input.
  */
  @Override
  protected void setup(Context context) {
    Configuration cfg = context.getConfiguration();
    String regex = cfg.get("SearchQuery.Regex");
    pattern = Pattern.compile(regex);
  }

  /**
  * Checks using Matcher for a valid regular expression, map a matching value to a matching key.
  * The key being the line number of the file the value was located in.
  *
  * @see java.util.regex.Matcher
  */
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    java.util.regex.Matcher m = pattern.matcher(line);

    int lineNumber = (int) key.get();

    while (m.find()) {
      outputKey.set(m.group());
      outputValue.set(" LineNumber: " + lineNumber + " FileName: "
          + ((FileSplit) context.getInputSplit()).getPath());
      context.write(outputKey, outputValue);
    }
  }
}
