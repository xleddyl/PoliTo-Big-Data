// Q1 -> B
// Q2 -> C

// file structure 1: PlantID,City,Country (PID1,Turin,Italy) - ProductionPlants
// file structure 2: RID,PlantID,IP (R15,PID1,130.192.20.21) - Robots
// file structure 3: RID,FailureTypeCode,Date,Time (R15,FCode122,2020/05/01,06:40:51) - Failures

// E1 -> MapReduce and Hadoop
// compute number of robots with failure FCode122 at least one time
class MapperEX extends Mapper< ... > {
   protected void map(LongWritable key, Text value, Context context) ... {
      String[] fields = value.toString().split(",");
      String code = fields[1];
      String RID = fields[0];
      if(code.equals("FCode122")) {
         context.write(new Text(RID), NullWritable.key());
      }
   }
}

class ReducerEX extends Reducer< ... > {   // only 1 reducer
   int tot;
   protected void setup(Context context) {
      tot = 0;
   }
   protected void reduce(Text key, Iterable<NullWritable>, Context context) ... {
      tot ++;
   }
   protected void cleanup(Context context) {
      context.write(new IntWritable(tot), NullWritable.get());
   }
}

// E2 -> Spark and RDDs
// A -> select PlantIDs with at least one robot associated with >=50 failures in 2020 -> (PlantID)
// B -> compute for each PlantIDs the number of robots associated with >=1 failures in 2020 -> (PlantID, nRobots)
class SparkDriver {
   public static void main( ... ) ... {
      // init

      JavaRDD<String> productionPlants = sc.textFile(input1);
         .map(line -> line.split(",")[0])
      JavaPairRDD<String, String> robots = sc.textFile(input2)
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String RID = fields[0];
            String PlantID = fields[1];
            return new Tuple2<String, String>(RID, PlantID);
         });
      JavaRDD<String> failures = sc.textFile(input3)
         .filter(line -> line.split(",")[2].split("/")[0].equals("2020"));

      // A
      JavaPairRDD<String, Integer> failuresPerRobot = failures
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String RID = fields[0];
            return new Tuple2<String, Integer>(RID, 1);
         })   // (RID, 1)
         .reduceByKey((v1, v2) -> v1 + v2);
      JavaRDD<String> plantsWithFaultyRobots = failures
         .join(robots)   // (RID, (failures, PlantID))
         .filter(pair -> pair._2()._1() >= 50);
         .map(pair -> pair._2()._2())
         .distinct();
      plantsWithFaultyRobots.saveAsTextFile(ouptutA);

      // B
      JavaPairRDD<String, Integer> plantsCount = failuresPerRobot
         .join(robots)   // (RID, (failures, PlantID))
         .mapToPair(pair -> new Tuple2<String, Integer>(pair._2()._2(), 1));
         .reduceByKey((v1, v2) -> v1 + v2);   // (PlantID, nRobots with failures)
      JavaPairRDD<String, Integer> plantsZero = productionPlants
         .mapToPair(line -> new Tuple2<String, Integer>(line, 1))
         .subtractByKey(plantsCount);
      JavaPairRDD<String, Integer> plantsReport = plantsCount.union(plantsZero);
      plantsReport.saveAsTextFile(outputB);
   }
}