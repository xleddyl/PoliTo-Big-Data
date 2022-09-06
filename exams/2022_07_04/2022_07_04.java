// Q1 -> C
// Q2 -> D

// file structure 1: SID,OperatingSystem,Model (S10,Ubuntu6,SunUltraServer1) - Servers
// file structure 2: PID,ReleaseDate,OperatingSystem (PIDW10_22,2022/01/18,Ubuntu6) - Patches
// file structure 3: PID,SID,ApplicationDate (PIDW10_22,S10,2022/02/21) - AppliedPatches

// E1 -> MapReduce and Hadoop
// (ReleaseDate from 2021/07/04 to 2022/07/03) select OS with highes n of patches (alphabetical order if conflicts) -> (OperatingSystem)
class MapperEX extends Mapper< ... > {   // file Patches
   protected void map(LongWritable key, Text value, Context context) ... {
      String[] fields = value.toString().split(",");
      String date = fields[1];
      String OperatingSystem = fields[2];
      if(date.compareTo("2021/07/04") > 0 && date.compareTo("2022/07/03") < 0) {
         context.write(new Text(OperatingSystem), NullWritable.get());
      }
   }
}

class ReducerEX extends Reducer< ... > {
   String OS;
   int max;
   protected void setup(Context context) {
      OS = new String("zzz");
      patches = -1;
   }

   protected void reduce(Text key, Iterable<NullWritable> values, Context context) {
      int patches
      for(NullWritable value : values) {
         patches ++;
      }
      if(patches > max) {
         max = patches;
         OS = key.toString();
      } else if (pathces == max && key.toString().compareTo(OS) < 0) {
         max = patches;
         OS = key.toString();
      }
   }

   protected void setup(Context context) {
      context.write(new Text(OS), NullWritable.get());
   }
}

// E2 -> Spark and RDDs
// A -> (OS = Ubuntu 2) select patches applied on >= 100 server with ReleaseDate = ApplicationDate -> (PID)
// B -> compute for each server the n of months of 2021 with no patches (also 0 months) -> (SID, nOfMonthsWithNoPatches)
class SparkDriver {
   public static void main( ... ) ... {
      // init

      JavaRDD<String> servers = sc.textFile(input1).cache();
      JavaRDD<String> pathces = sc.textFile(input2).cache();
      JavaRDD<String> appliedPatches = sc.textFile(input3).cache();

      // A
      JavaPairRDD<String, String> pathcesUbuntuReleaseDate = patches
         .filter(line -> line.split(",")[2].equals("Ubuntu 2"))
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String PID = fields[0];
            String ReleaseDate = fields[1];
            return new Tuple2<>(PID, ReleaseDate);
         })   // (PID, ReleaseDate)
         .cache();
      JavaPairRDD<String, String> patchesApplicationDate = appliedPatches
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String PID = fields[0];
            String ApplicationDate = fields[2];
            return new Tuple2<>(PID, ApplicationDate);
         })   // (PID, ApplicationDate)
         .cache();
      JavaPairRDD<String, Integer> patchesAppliedSameDay = pathcesUbuntuReleaseDate
         .join(patchesApplicationDate)   // (PID, (ReleaseDate, ApplicationDate))
         .filter(pair -> {
            String ReleaseDate = pair._2()._1();
            String ApplicationDate = pair._2()._2();
            return ReleaseDate.equals(ApplicationDate);
         })
         .mapValues(val -> 1)
         .reduceByKey((v1, v2) -> v1 + v2)
         .filter(pair -> pair._2() >= 100)
         .keys();   // (PID)
      patchesAppliedSameDay.saveAsTextFile(outputA);

      // B
      JavaPairRDD<String, Integer> nOfMonth2021WithoutPatchesPARTIAL = appliedPatches
         .filter(line -> line.split(",")[2].split("/")[0].equals("2021"))
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String SID = fields[1];
            String month = fields[2].split("/")[1];
            return new Tuple2<>(SID, month);
         })   // (SID, month)
         .distinct()
         .groupByKey()
         .mapValues(val -> {
            int months = 12
            for(int m : val) {
               months --;
            }
            return months;
         });   // (SID, nOfMonthsWithoutPatches)
      JavaPairRDD<String, Integer> nOfMonth2021WithoutPatchesTOTAL = servers
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String SID = fields[0];
            return new Tuple2<>(SID, 12);
         })
         .leftOuterJoin(nOfMonth2021WithoutPatchesPARTIAL)   // (SID, (12, Optional(nOfMonthsWithoutPatches)))
         .mapValues(val -> val._2().isPresent() ? val._2().get() : val._1())   // (SID, nOfMonthsWithoutPatches)
      nOfMonth2021WithoutPatches.saveAsTextFile(outputB);

   }
}