package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Word count problem
* @input: HDFS folder containing textual files
* @output: number of occurrences of each word appearing in at least one file
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
      job.setJobName("E02");

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
class MapperEX extends Mapper<LongWritable, Text, Text, IntWritable> {
   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] words = value.toStrin().split("\\s+");
      for(String word : words) {
         String cleanedWord = word.toLowerCase();
         context.write(new Text(cleanedWord), new IntWritable(1));
      }
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
class ReducerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
   protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int occurrences = 0;
      for(IntWritable value : values) {
         occurrences = occurrences + value.get()
      }
      context.write(key, new IntWritable(occurrences));
   }
}
