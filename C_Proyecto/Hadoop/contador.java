import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class reservasPorFecha {
  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text reservas = new Text();private static final IntWritable one = new IntWritable(1);
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] data = value.toString().split(",");
      if (data.length >= 3) {
        reservas.set(data[2]);
        context.write(reservas, one);
      } 
    }
} 
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
  private IntWritable result = new IntWritable();
  @Override
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
public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "reservasPorFecha");
  job.setJarByClass(reservasPorFecha.class);
  job.setMapperClass(Map.class);
  job.setCombinerClass(Reduce.class);
  job.setReducerClass(Reduce.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}