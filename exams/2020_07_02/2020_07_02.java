// Q1 -> B
// Q2 -> A

// file structure 1: SID,Model (S10,SunP20)
// file structure 2: SID,PID,Date (S10,P12,2019/08/05)

// E1 -> MapReduce and Hadoop
// select SIDs with number of patches >= 30 in 2017 or 2018
class MapperEX extends Mapper< ... > {
   protected void map(LongWritable key, Text value, Context context) ... {
      String[] fields = value.toString().split(",");
      String year = fields[2].split("/")[0];
      if(year.equals("2017") || year.equals("2018")) {
         context.write(new Text(fields[0]), new Text(year));
      }
   }
}

class ReducerEX extends Reducer< ... > {
   protected void reduce(Text key, Iterable<Text> values, Context context) ... {
      int y2017 = 0;
      int y2018 = 0;
      for(Text value : values) {
         if(value.toString().equals("2017")) {
            y2017 ++;
         } else {
            y2018 ++;
         }
      }
      if(y2017 >= 30 || y2018 >= 30) {
         context.write(key, NullWritable.get());
      }
   }
}

// E2 -> Spark and RDDs
// A -> select SIDs if nPatch 2019 <= 50% nPatch 2018 (nP2019 < 0.5 * nP2018). ouptut: SID,model
// B -> select SIDs nPatch <= 1 for each date (0 or 1). output: SID,model. prints: number of distinct models
class SparkDriver {
   public static void main( ... ) ... {
      // variables
      // JavaSparkContext sc
      // SparkSession ss

      JavaPairRDD<String> servers = sc.textFile(serverInput)
         .mapToPair(line -> {
            String[] fields = line.split(",");
            return new Tuple2<String, String>(fields[0], fields[1]);
         });
      JavaRDD<String> patches = sc.textFile(patchedInput);

      // A
      JavaPairRDD<String, String> serversWith50PercDecrease = patches
         .filter(line -> {
            String[] year = line.split(",")[2].split("/")[0];
            return (date.equals("2018") || date.equals("2019"));
         })
         .mapToPair(line -> {
            YearCount yc;
            String[] fields = line.split(",");
            String year = fields[2].split("/")[0];
            String SID = fields[0];
            yc = year.equals("2018") ? new YearCount(0, 1) : new YearCount(1, 0);
            return new Tuple2<String, YearCount>(SID, yc);
         })
         .reduceByKey((v1, v2) -> new YearCount(
            v1.getNP2019() + v2.getNP2019(),
            v1.getNP2018() + v2.getNP2018(),
         ))
         .filter(pair -> {
            int nP2019 = pair._2().getNP2019()
            int nP2018 = pair._2().getNP2018()
            return nP2019 < 0.5 * nP2018
         })
         .join(servers)
         .mapToPair(pair -> new Tuple2<String, String>(pair._1(), pair._2()._2()));
      serversWith50PercDecrease.saveAsTextFile(outputA);

      // B
      JavaPairRDD<String, String> serversToBeDiscarded = patches
         .mapToPair(line -> {
            String[] fields = line.split(",");
            return new Tuple2<String, Integer>(fields[0] + "_" + fields[2], 1);
         })
         .reduceByKey((v1, v2) -> v1 + v2)
         .filter(pair -> pair._2() > 1)
         .mapToPair(pair -> {
            String[] fields = pair._1().split("_");
            return new Tuple2<String, Integer>(fields[0], pair._2());
         });
      JavaPairRDD<String, String> serversWithLT1PathcPerDay = servers
         .subtractByKey(serversToBeDiscarded);
      serversWithLT1PathcPerDay.saveAsTextFile(outputB);
      System.out.println(serversWithLT1PathcPerDay.values().distinct().count());
   }
}

class YearCount {
   private int nP2019;
   private int nP2018;
   // setters, getters and constructor
}