package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Select outliers
* @input: a collection of structured textual files containing the daily value of PM10 for a set of sensors
* @output: the records with a PM10 value below a threshold (given as arg)
*/

// DRIVER class
public class DriverEX extends Configured implements Tool {
   @Override
   public int run(String[] args) throws Exception {
      Path inputPath = new Path(args[0]);
      Path outputDir = new Path(args[1]);
      int numberOfReducers = Integer.parseInt(args[3]); // 0

      // create job
      Configuration conf = this.getConf();
      conf.set("maxThreshold", args[2])
      Job job = Job.getInstance(conf);
      job.setJobName("E12");

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
      job.setMapOutputValueClass(FloatWritable.class);

      // set combiner class
      // job.setCombinerClass(CombinerEX.class);

      // set reducer class
      // job.setreducerClass(ReducerEX.class);
      // job.setOutputKeyClass(Text.class);
      // job.setOutputValueClass(FloatWritable.class);
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
class MapperEX extends Mapper<LongWritable, Text, Text, IntWritable> {
   float threshold;

   protected void setup(Context context) {
      threshold = Float.parseFloat(context.getConfiguration().get("maxThreshold")):
   }

   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      float measure = Float.parseFloat(value.toString());
      if(measure < threshold) {
         context.write(new Text(key), new FloatWritable(measure));
      }
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
