package it.polito.xleddyl.mapreduce
// imports omitted

/**
* PM10 pollution analysis
* @input: structured textual file containing the daily value of PM10 for a set of sensors
* @output: report for each sensor the number of days with PM10 above a specific threshold (50)
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
      job.setJobName("E03");

      // specify input/output folder/file
      FileInputFormat.addInputPath(job, inputPath);
      FileInputFormat.setOutputPath(job, outputDir);

      // add input/output to job
      job.setInputFormatClass(KeyValueTextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      // set driver class
      job.setJarByClass(DriverEX.class);

      // set mapper class
      job.setMapperClass(MapperEX.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);

      // set reducer class
      job.setreducerClass(ReducerEX.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
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
class MapperEX extends Mapper<Text, Text, Text, IntWritable> {
   private static Double PM10Threshold = new Double(50);

   protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      String[] fields = key.toString().split(",");
      String sensor_id = fields[0];
      Double PM10Level = new Double(value.toString());
      if(PM10Level.compareTo(PM10Threshold) > 0) {
         context.write(new Text(sensor_id), new IntWritable(1));
      }
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
class ReducerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
   protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int numDays = 0;
      for(IntWritable value : values) {
         numDays = numDays + value.get();
      }
      context.write(key, new IntWritable(numDays));
   }
}
