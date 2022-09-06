package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Categorization rules
* @input: large file containing a set of recors (info about user) + small file with set of business rules that are used to assign each user to a category
* @output: One record per user with the format: original info + category (unknown if no match)
*/

// DRIVER class
public class DriverEX extends Configured implements Tool {
   @Override
   public int run(String[] args) throws Exception {
      Path inputPath = new Path(args[1]);
      Path outputDir = new Path(args[2]);
      Path businessRulesPath = new Path(args[3]);
      int numberOfReducers = Integer.parseInt(args[0]); // 0

      // create job
      Configuration conf = this.getConf();
      Job job = Job.getInstance(conf);
      job.setJobName("E27");
      job.addCacheFile(businessRulesPath.toUri());

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
   HashMap<String, ArrayList<String>> businessRules;

   protected void setup(Context context) {
      String line;
      businessRules = new HashMap<Integer, ArrayList<String>>();
      Path businessRulesPath = (context.getCacheFiles()[0]).getPath();
      BufferedReader file = new BufferedReader(new FileReader(new File(businessRulesPath)));
      while((line = file.readLine()) != null) {
         String[] split1 = line.split("->");
         String[] split2 = split1[0].split("and");
         String gender = split2[0].split("=")[1];
         String year = split2[1].split("=")[1];
         businessRules.put(new String(split1[1]), new ArrayList<String>(new String(gender.toLowerCase()), new String(year)));
      }
   }

   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] fields = value.toString().split(",");
      boolean flag = false;

      for(Entry<String, ArrayList<String> pair : businessRule.entrySet()) {
         if(pair.getValue().contains(fields[3].toLowerCase()) && pair.getValue().contains(fields[4])) {
            context.write(new Text(value.toString + "," + pair.getKey()), NullWritable.get());
            flag = true;
            break;
         }
      }
      if(!flag) {
         context.write(new Text(value.toString + ",Unknown"), NullWritable.get());
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
