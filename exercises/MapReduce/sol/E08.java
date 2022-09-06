package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Total income
* @input: structired textual csv files containing the daily income of a company
* @output: total income for each month of the year + average monthly income for each year considering only the months with a total income > 0
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
      job.setJobName("E08");

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
      job.setMapOutputValueClass(MonthIncome.class);

      // set combiner class
      // job.setCombinerClass(CombinerEX.class);

      // set reducer class
      job.setreducerClass(ReducerEX.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
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
class MapperEX extends Mapper<Text, Text, Text, MonthIncome> {
   protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      MonthIncome monthIncome = new MonthIncome();
      String[] date = key.toString().split("-");
      monthIncome.setMonthID(date[1]);
      monthIncome.setIncome(Double.parseDouble(value.toString()));
      context.write(new Text(date[0]), monthIncome);
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
class ReducerEX extends Reducer<Text, MonthIncome, Text, DoubleWritable> {
   protected void reduce(Text key, Iterable<MonthIncome> values, Context context) throws IOException, InterruptedException {
      double totalYearlyIncome = 0;
      int countMonths = 0;
      HashMap<String, Double> totalMonthIncome = new HashMap<String, Double>();
      String year = key.toString();

      for(MonthIncome value : values) {
         Double income = totalMonthIncome.get(value.getMonthID());
         totalMonthIncome.put(new String(value.getMonthID()), new Double(value.getIncome()) + (income != null ? income : 0));
         countMonths ++;
         totalYearlyIncome += value.getIncome();
      }

      context.write(new Text(year), new DoubleWritable(totalYearlyIncome / countMonths));
      for(Entry<String, Double> pair : totalMonthIncome.entrySet()) {
         context.write(new Text(year + "-" + pair.getKey()), new DoubleWritable(pair.getValue()));
      }
   }
}

// ----------------------------------------------------------------
public class MonthIncome implements Writable {
   private String monthID;
   private double income;

   // getters and setters

   @Override
   public void readFields(DataInput in) throws IOException {
      monthID = in.readUTF();
      income = in.readDouble();
   }

   @Override
   public void write(DataOutput out) throws IOException {
      out.writeUTF(monthID);
      out.writeDouble(income);
   }
}

// ----------------------------------------------------------------
// COMBINER class       Reducer<input k, input v, output k, output v>
// class CombinerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    }
// }

