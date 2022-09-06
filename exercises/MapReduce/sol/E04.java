package it.polito.xleddyl.mapreduce
// imports omitted

/**
* PM10 pollution analysis per city zone
* @input: structured textual file containing the daily value of PM10 for a set of city zones
* @output: report for each zone the list of dates associated with PM10 value above a specific threshold (50)
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
      job.setJobName("E04");

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
      job.setMapOutputValueClass(Text.class);

      // set reducer class
      job.setreducerClass(ReducerEX.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
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
class MapperEX extends Mapper<Text, Text, Text, Text> {
   private static Double PM10Threshold = new Double(50);

   protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      String[] fields = key.toString().split(",");
      String zone = fields[0];
      String date = fields[1];
      Double PM10Level = new Double(value.toString());
      if(PM10Level > PM10Threshold) {
         context.write(new Text(zone), new Text(date));
      }
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
class ReducerEX extends Reducer<Text, Text, Text, Text> {
   protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String dates = new String("");
      for(String date : values) {
         dates.concat((dates.length() == 0 ? "" : ",") + date.toString());
      }
      context.write(key, new Text(dates));
   }
}
