package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Total count
* @input: a collection of structured textual csv files containing the daily value of PM10 for a set of sensors
* @output: total number of records
*/

// DRIVER class
public class DriverEX extends Configured implements Tool {
   public static enum COUNTERS {
      TOTAL_RECORDS
   }

   @Override
   public int run(String[] args) throws Exception {
      Path inputPath = new Path(args[1]);
      Path outputDir = new Path(args[2]);
      int numberOfReducers = Integer.parseInt(args[0]); // 0

      // create job
      Configuration conf = this.getConf();
      Job job = Job.getInstance(conf);
      job.setJobName("E10");

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
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(NullWritable.class);

      // set combiner class
      // job.setCombinerClass(CombinerEX.class);

      // set reducer class
      // job.setreducerClass(ReducerEX.class);
      // job.setOutputKeyClass(Text.class); // to be modified
      // job.setOutputValueClass(IntWritable.class); // to be modified
      job.setNumReduceTasks(0);

      int exitCode = (job.waitForCompletion(true) == true ? 0 : 1)
      Counter totRecords = job.getCounters().findCounter(COUNTERS.TOTAL_RECORDS);
      System.out.println("total: " + totRecords.getValue());
      return exitCode;
   }

   public static void main(String args[]) throws Exception {
      int res = ToolRunner.run(new Configuration(), new DriverEX(), args);
      System.exit(res)
   }
}

// ----------------------------------------------------------------
// MAPPER class        Mapper<input k, input v, output k, output v>
class MapperEX extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.getCounter(COUNTERS.TOTAL_RECORDS).increment(1);
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
// class ReducerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    }
// }

// ----------------------------------------------------------------
// COMBINER class       Reducer<input k, input v, output k, output v>
// class CombinerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    }
// }
