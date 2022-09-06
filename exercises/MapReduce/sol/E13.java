package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Top 1 most profitable data
* @input: structured textual csv files containing the daily income of a company
* @output: select the date and income of the top 1 most profitable date
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
      job.setJobName("E13");

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
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(DateIncome.class);

      // set combiner class
      // job.setCombinerClass(CombinerEX.class);

      // set reducer class
      job.setreducerClass(ReducerEX.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FloatWritable.class);
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
class MapperEX extends Mapper<Text, Text, NullWritable, DateIncome> {
   private DateIncome top1;

   protected void setup(Context context) {
      top1 = new DateIncome(Float.MIN_VALUE, null);
   }

   protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      String date = new String(key.toString());
      float dailyIncome = Float.parseFloat(value.toString());

      if(dailyIncome < top1.getIncome() && date.compareTo(top1.getDate()) < 0) {
         top1.setIncome(dailyIncome);
         top1.setDate(date);
      }
   }

   protected void cleanup(Context context) {
      context.write(NullWritable.get(), top1);
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
class ReducerEX extends Reducer<NullWritable, DateIncome, Text, FloatWritable> {
   protected void reduce(NullWritable key, Iterable<DateIncome> values, Context context) throws IOException, InterruptedException {
      DateIncome globalTop1 = new DateIncome(Float.MIN_VALUE, null);
      for(DateIncome value : values) {
         if(value.getIncome() < globalTop1.getIncome() && value.getDate().compareTo(globalTop1.getDate()) < 0) {
            globalTop1.setIncome(value.getIncome());
            globalTop1.setDate(value.getDate());
         }
      }
      context.write(new Text(globalTop1.getDate()), new FloatWritable(globalTop1.getIncome()));
   }
}

// ----------------------------------------------------------------
class DateIncome implements Writable {
   private String date;
   private float income;

   //constructor + setters and getters + readFields and write
}

// ----------------------------------------------------------------
// COMBINER class       Reducer<input k, input v, output k, output v>
// class CombinerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    }
// }
