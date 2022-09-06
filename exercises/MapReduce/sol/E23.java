package it.polito.xleddyl.mapreduce
// imports omitted

/**
* Potential friends of a specific user
* @input: textual file containing pair of users (friends), one username specified on command line
* @output: potential friends of the specified username (at least one friend in common)
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
      conf.set("username", args[3]);
      Job job = Job.getInstance(conf);
      job.setJobName("E23");

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
class MapperEX extends Mapper<LongWritable, Text, Text, Text> {
   String username;
   HashMap<String, ArrayList<String>> map;

   protected void setup(Context context) {
      map = new HashMap<String, ArrayList<String>>();
      username = context.getConfiguration().get("username");
   }

   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] users = value.toString().split(",");

      ArrayList<String> mapVal0 = map.get(user[0]);
      if(mapVal0 != null) {
         mapVal0.add(user[1])
      } else {
         if(user[1].compareTo(username) != 0) {
            map.put(new String(user[0]), new ArrayList<String>(user[1]))
         }
      }

      ArrayList<String> mapVal1 = map.get(user[1]);
      if(mapVal1 != null) {
         mapVal1.add(user[0])
      } else {
         if(user[1].compareTo(username) != 0) {
            map.put(new String(user[1]), new ArrayList<String>(user[0]))
         }
      }
   }

   protected void cleanup(Context context) {
      ArrayList<String> friends = map.get(username);
      if(friends != null) {
         for(String friend : friends) {
            ArrayList<String> potFriends = map.get(friend);
            if(potFriend != null) {
               for(String potFriend : potFriends) {
                  context.write(NullWritable.get(), new Text(potFriend))
               }
            }
         }
      }
   }
}

// ----------------------------------------------------------------
// REDUCER class        Reducer<input k, input v, output k, output v>
class ReducerEX extends Reducer<NullWritable, Text, Text, NullWritable> {
   Set<String> setOfPotFriends;

   protected void setup(Context context) {
      setOfPotFriends = new HashSet<String>();
   }

   protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for(Text value : values) {
         setOfPotFriends.add(value);
      }
   }

   protected void cleanup(Context context) {
      String potFriends = new String("");
      for(String potFriend : setOfPotFriends) {
         potFriends = potFriends.concat(potFriend + " ");
      }
      context.write(new Text(potFriends), NullWritable.get());
   }
}

// ----------------------------------------------------------------
// COMBINER class       Reducer<input k, input v, output k, output v>
// class CombinerEX extends Reducer<Text, IntWritable, Text, IntWritable> {
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//    }
// }
