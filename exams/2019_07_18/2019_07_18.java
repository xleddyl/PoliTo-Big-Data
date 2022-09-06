// Q1 -> B
// Q2 -> B

// file structure: PID,Date,ApplicationName,BriefDescription (PID7000,2017/10/01,Windows 10,Security patch)

// E1 -> MapReduce and Hadoop
// select software app with number of patches in 2018 > patches in 2017
class MapperEX extends Mapper< ... > {
   protected void map(LongWritable key, Text value, Context context) ... {
      String[] fields = value.toString().split(",");
      String year = fields[1].split("/")[0];
      if(year.equals("2018")) {
         context.write(new Text(fields[2]), new IntWritable(1));
      }
      if(year.equals("2017")) {
         context.write(new Text(fields[2]), new IntWritable(-1));
      }
   }
}

class ReducerEX extends Reducer< ... > {
   protected void reduce(Text key, Iterable<IntWritable> values, Context context) ... {
      int difference = 0;
      for(IntWritable value : values) {
         difference += value.get();
      }
      if(difference > 0) {
         context.write(key);
      }
   }
}

// E2 -> Spark and RDDs
// A -> select for each month of 2017 the application with more patches between WIN10 and UBUNTU1804
// B -> select the window of 3 months in 2018 such that in each month of the window there are 4 patches for the app
class SparkDriver {
   public static void main( ... ) ... {
      // variables
      // JavaSparkContext sc
      // SparkSession ss

      JavaRDD<String> patches = sc.textFile(input)

      // A
      JavaPairRDD<String, String> appWithMorePatchesPerMonth = patches
         .filter(line -> {
            String[] fields = line.split(",");
            String date = fields[1].split("/")[0];
            String app = fields[2];
            return (date.equals("2017") && (app.equals("Windows 10") || app.equals("Ubuntu 18.04")));
         })
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String month = fields[1].split("/")[1];
            String app = fields[2];
            return new Tuple2<String, AppPatch>(month, app.equals("Windows 10") ? 1 : -1);
         })
         .reduceByKey((v1 , v2) -> v1 + v2)
         .filter(pair -> pair != 0)
         .mapToPair(pair -> {
            String out = pair._2() > 0 ? new String("W") : new String("U");
            new Tuple2<String, String>(pair._1(), out);
         })
         .distinct();
      appWithMorePatchesPerMonth.saveAsTextFile(outputA);

      // B
      JavaPairRDD<Integer, String> windowsOfThreeMonths = patches
         .filter(line -> line.split(",")[1].split("/")[0].equals("2018"))
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String month = Integer.parseInt(fields[1].split("/")[1]);
            String app = fields[2];
            MonthApp ma = MonthCount(month, app);
            return new Tuple2<MonthApp, Integer>(ma, 1);
         })
         .reduceByKey((v1, v2) -> v1 + v2)
         .filter(pair -> pair._2() >= 4)
         .flatMapToPair(pair -> {
            ArrayList<Tuple2<MonthApp, Integer>> flatMap = new ArrayList<Tuple2<MonthApp, Integer>>();
            String month = pair._1().getMonth();
            String app = pair._1().getApp();
            flatMap.add(new Tuple2<MonthApp, Integer>(new MonthApp(month, app)), 1);
            if(month - 1 > 0) {
               flatMap.add(new Tuple2<MonthApp, Integer>(new MonthApp(month - 1, app)), 1);
            }
            if(month - 2 > 0) {
               flatMap.add(new Tuple2<MonthApp, Integer>(new MonthApp(month - 2, app)), 1);
            }
            return flatMap.iterator();
         })
         .reduceByKey((v1, v2) -> v1 + v2)
         .filter(pair -> pair._2() == 3)
         .mapToPair(pair -> {
            int month = Stripair._1().getMonth();
            String app = Stripair._1().getApp();
            return new Tuple2<Integer, String>(month, app);
         });
      windowsOfThreeMonths.saveAsTextFile(outputB);
   }
}

class MonthApp {
   private int month;
   private String app;
   // setters, getters and constructor
}