package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Word to integer conversion
* @input: a large textual file + a small dictionary containing the mapping of each word
* @output: a textual file containing the converted version of the file
*/

// DRIVER class
public class DriverEX extends Configured implements Tool {
   @Override
   public int run(String[] args) throws Exception {
      Path inputPath = new Path(args[1]);
      Path outputDir = new Path(args[2]);
      Path dictionaryPath = new Path(args[3]);
      int numberOfReducers = Integer.parseInt(args[0]); // 0

      // create job
      Configuration conf = this.getConf();
      Job job = Job.getInstance(conf);
      job.setJobName("E26");
      job.addCacheFile(dictionaryPath.toUri());

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
      job.setMapOutputValueClass(Text.class);

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
class MapperEX extends Mapper<LongWritable, Text, NullWritable, Text> {
   HashMap<String, Integer> dictionary;

   private void setup(Context context) {
      String line;
      dictionary = new HashMap<String, Integer>();
      Path dictionaryPath = (context.getCacheFiles()[0]).getPath();
      BufferedReader file = new BufferedReader(new FileReader(new File(dictionaryPath)));
      while((line = file.readLine()) != null) {
         String[] fields = line.split("\t");
         dictionary.put(new String(fields[1].toLowerCase()), Integer.parseInt(fields[0]));
      }
      file.close();
   }

   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String sentence = String("");
      String[] words = value.toString().split("\\s+");
      for(String word : words) {
         sentence = sentence.concat(dictionary.get(word.toLowerCase()) + " ");
      }
      context.write(NullWritable.get(), new Text(sentence));
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
