import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Apriori {

  // Mapper class
  public static class AprioriMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] items = value.toString().split(" ");
      for (String item : items) {
        context.write(new Text(item), one);
      }
    }
  }

  // Reducer class
  public static class AprioriReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int minSupport;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      minSupport = conf.getInt("minSupport", 2);
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if (sum >= minSupport) {
        context.write(key, new IntWritable(sum));
      }
    }
  }

  // Driver code
  public static void main(String[] args) throws Exception {
    // Set minimum support threshold
    int minSupport = 2;

    Configuration conf = new Configuration();
    conf.setInt("minSupport", minSupport);

    Job job = Job.getInstance(conf, "Apriori");

    job.setJarByClass(Apriori.class);
    job.setMapperClass(AprioriMapper.class);
    job.setCombinerClass(AprioriReducer.class);
    job.setReducerClass(AprioriReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
