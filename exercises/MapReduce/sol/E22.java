package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Friends of a specific user
* @input: textual file containing pair of users (friends), one username specified on command line
* @output: the friends of the specified username stored in a textual file
*/

// DRIVER class
public class DriverEX extends Configured implements Tool {
   @Override
   public int run(String[] args) throws Exception {
      Path inputPath = new Path(args[1]);
      Path outputDir = new Path(args[2]);
      int numberOfReducers = Integer.parseInt(args[0]); // 1

      // create job
      Configuration conf = this.getConf();
      conf.set("username", args[3])
      Job job = Job.getInstance(conf);
      job.setJobName("E22");

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
      job.setreducerClass(ReducerEX.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
      job.setNumReduceTasks(1);

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
   String username;

   protected void setup(Context context) {
      username = context.getConfiguration().get("username");
   }

   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] users = value.toString().split(",");
      if(username.compareTo(users[0]) == 0) {
         context.write(NullWritable.get(), new Text(users[1]));
      }
      if(username.compareTo(users[0]) == 1) {
         context.write(NullWritable.get(), new Text(users[0]));
      }
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
class ReducerEX extends Reducer<NullWritable, Text, Text, NullWritable> {
   protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String friends = new String("");
      for(String value : values) {
         friends = friends.concat(value.toString() + " ");
      }
      context.write(new Text(friends), NullWritable.get());
   }
}

// ----------------------------------------------------------------
// COMBINER class       Reducer<input k, input v, output k, output v>
// class CombinerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    }
// }
