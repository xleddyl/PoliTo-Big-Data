package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Mapping QA
* @input: two large textual file containing a one set of questions and one a set of answers
* @output: one line per pair with the following format (see pdf)
*/

// DRIVER class
public class DriverEX extends Configured implements Tool {
   @Override
   public int run(String[] args) throws Exception {
      Path inputPath1 = new Path(args[1]);
      Path inputPath2 = new Path(args[2]);
      Path outputDir = new Path(args[3]);
      int numberOfReducers = Integer.parseInt(args[0]); // 1 or more

      // create job
      Configuration conf = this.getConf();
      Job job = Job.getInstance(conf);
      job.setJobName("E28");

      // specify input/output folder/file
      MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, MapperEX1.class)
      MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, MapperEX2.class)
      // FileInputFormat.addInputPath(job, inputPath);
      FileInputFormat.setOutputPath(job, outputDir);

      // add input/output to job
      job.setInputFormatClass(TextInputFormat.class);
      // job.setOutputFormatClass(TextOutputFormat.class);

      // set driver class
      job.setJarByClass(DriverEX.class);

      // set mapper class
      job.setMapperClass(MapperEX.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);

      // set combiner class
      // job.setCombinerClass(CombinerEX.class);

      // set reducer class
      job.setreducerClass(ReducerEX.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
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
class MapperEX1 extends Mapper<LongWritable, Text, Text, Text> {
   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] fields = value.toString().split(",");
      context.write(new Text(fields[0]), new Text(fields[2]));
   }
}

class MapperEX2 extends Mapper<LongWritable, Text, Text, Text> {
   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] fields = value.toString().split(",");
      context.write(new Text(fields[1]), new Text(fields[0] + "," + fields[3]));
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
class ReducerEX extends Reducer<Text, Text, Text, NullWritable> {
   protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for(Text value : values) {
         context.write(new Text(key.toString() + "," + value), NullWritable.get());
      }
   }
}

// ----------------------------------------------------------------
// COMBINER class       Reducer<input k, input v, output k, output v>
// class CombinerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    }
// }
