package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Split readings of a set of sensors based on the value of the measurement
* @input: a set of textual files containing the temperatures gathered by a set of sensors
* @output: a set of files with prefix "high-temp" (> 30.0) and "normal-temp" (<= 30.0)
*/

// DRIVER class
public class DriverEX extends Configured implements Tool {
   @Override
   public int run(String[] args) throws Exception {
      Path inputPath = new Path(args[1]);
      Path outputDir = new Path(args[2]);
      int numberOfReducers = Integer.parseInt(args[0]); // 0

      // create job
      Configuration conf = this.getConf();
      Job job = Job.getInstance(conf);
      job.setJobName("E20");

      // specify input/output folder/file
      FileInputFormat.addInputPath(job, inputPath);
      FileInputFormat.setOutputPath(job, outputDir);

      // add input/output to job
      job.setInputFormatClass(TextInputFormat.class);
      MultipleOutputs.addNamedOutput(job, "normal-temp", TextOutputFormat.class, Text.class, NullWritable.class);
      MultipleOutputs.addNamedOutput(job, "high-temp", TextOutputFormat.class, Text.class, NullWritable.class);
      // job.setOutputFormatClass(TextOutputFormat.class);

      // set driver class
      job.setJarByClass(DriverEX.class);

      // set mapper class
      job.setMapperClass(MapperEX.class);
      // job.setMapOutputKeyClass(NullWritable.class);
      // job.setMapOutputValueClass(Text.class);

      // set combiner class
      // job.setCombinerClass(CombinerEX.class);

      // set reducer class
      // job.setreducerClass(ReducerEX.class);
      // job.setOutputKeyClass(Text.class); // to be modified
      // job.setOutputValueClass(IntWritable.class); // to be modified
      job.setNumReduceTasks(0);

      return (job.waitForCompletion(true) == true ? 0 : 1)
   }

   public static void main(String args[]) throws Exception {
      int res = ToolRunner.run(new Configuration(), new DriverEX(), args);
      System.exit(res)
   }
}

// ----------------------------------------------------------------
// MAPPER class        Mapper<input k, input v, output k, output v>
class MapperEX extends Mapper<LongWritable, Text, Text, NullWritable> {
   private MultiOuputs<Text, NullWritable> mos = null

   protected void setup(Context context) {
      mos = new MultiOuputs<Text, NullWritable>(context);
   }

   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] fields = value.toString().split(",");
      float temperature = Float.parseFloat(fields[3]);
      if(temperature < Float.parseFloat("30.0")) {
         mos.write("high-temp", value, NullWritable.get());
      } else {
         mos.write("normal-temp", value, NullWritable.get());
      }
   }

   protected void cleanup(Context context) {
      mos.close();
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
// class ReducerEX extends Reducer<NullWritable, Text, NullWritable, Text> {
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    }
// }

// ----------------------------------------------------------------
// COMBINER class       Reducer<input k, input v, output k, output v>
// class CombinerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    }
// }
