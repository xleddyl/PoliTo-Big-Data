package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Word count problem
* @input: unstructured textual file
* @output: number of occurrences of each word appearing in input file (use in-mapper combiners)
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
      job.setJobName("E09");

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

      // set combiner class
      // job.setCombinerClass(CombinerEX.class);

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
   HashMap<String, Integer> wordsCounts;

   protected void setup(Context context) {
      wordsCounts = new HashMap<String, Integer>();
   }

   protected void cleanup(Context context) {
      for(Entry<String, Integer> pair : wordsCounts.entrySet()) {
         context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
      }
   }

   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Integer currentFreq;
      String[] words = value.toString().split("\\s+");
      for(String word : words) {
         String cleaned = word.toLowerCase();
         currentFreq = wordsCounts.get(cleaned);
         wordsCounts.put(new String(cleaned), new Integer(currentFreq != null ? (currentFreq + 1) : 1));
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

// ----------------------------------------------------------------
// COMBINER class       Reducer<input k, input v, output k, output v>
// class CombinerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    }
// }
