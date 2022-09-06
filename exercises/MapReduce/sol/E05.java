package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Average
* @input: a collection of structured textual csv files containing the daily value of PM10 for a set of sensors
* @output: report for each sensor the average value of PM10
*/

// DRIVER class
public class DriverEX extends Configured implements Tool {
   @Override
   public int run(String[] args) throws Exception {
      Path inputPath = new Path(args[1]);
      Path outputDir = new Path(args[2]);
      int numberOfReducers = Integer.parseInt(args[0]); // 1 or more

      // create job
      Configuration conf = this.getConf();
      Job job = Job.getInstance(conf);
      job.setJobName("E05");

      // specify input/output folder/file
      FileInputFormat.addInputPath(job, inputPath);
      FileInputFormat.setOutputPath(job, outputDir);

      // add input/output to job
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      // set driver class
      job.setJarByClass(DriverEX.class);

      // set mapper class
      job.setMapperClass(MapperEX.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(FloatWritable.class);

      // set reducer class
      job.setreducerClass(ReducerEX.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FloatWritable.class);
      job.setNumReduceTasks(numberOfReducers);

      return (job.waitForCompletion(true) == true ? 0 : 1)
   }

   public static void main(String args[]) throws Exception {
      int res = ToolRunner.run(new Configuration(), new DriverEX(), args);
      System.exit(res)
   }
}

// ----------------------------------------------------------------
// MAPPER class        Mapper<input k, input v, output k, output v>
class MapperEX extends Mapper<LongWritable, Text, Text, FloatWritable> {
   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] fields = value.toString().split(",");
      String sensor_id = fields[0];
      float PM10value = Float.parseFloat(fields[2]);
      context.write(new Text(sensor_id), new FloatWritable(new Float(PM10value)));
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
class ReducerEX extends Reducer<Text, FloatWritable, Text, FloatWritable> {
   protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      int sum = 0;
      for(FloatWritable value : values) {
         sum = value.get();
         count ++;
      }
      context.write(key, new FloatWritable((float) sum / count));
   }
}