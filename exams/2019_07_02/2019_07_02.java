// Q1 -> A
// Q2 -> B

// E1 -> MapReduce and Hadoop
// select italian cities where all bicycles from same manufacturer
class MapperEX extends Mapper< ... > {
   protected void map(LongWritable key, Text value, Context context) ... {
      String[] fields = value.toString().split(",");
      if(fields[3].equals("Italy")) {
         context.write(new Text(fields[2]), new Text(fields[1]));
      }
   }
}

class ReducerEx extends Reducer< ... > {
   protected void reduce(Text key, Iterable<Text> values, Context context) ... {
      String prev = null;
      Boolean diff = false;

      for(Text value : values) {
         if(prev != null && !prev.equals(value.toString())) {
            diff = true;
            break;
         }
         prev = value.toString();
      }

      if(!diff) {
         context.write(key, new Text(prev));
      }
   }
}

// E2 -> Spark and RDDs
// A -> select bicycles with > 2 wheel failures in at least one month of 2018
// B -> select cities with <= 20 failures per bike in year 2018
class SparkDriver {
   public static void main( ... ) ... {
      // variables
      // JavaSparkContext sc
      // SparkSession ss

      JavaRDD<String> bicycles = sc.textFile(bicycleInput)
         .filter(line -> line.split(",")[2].equals("Italy"))
         .cache();
      JavaRDD<String> failures = sc.textFile(failuresInput)
         .filter(line -> line.split(",")[0].split("/")[0].equals("2018"))
         .cache();

      // A
      JavaRDD<String> wheelFailuresPerMonth = failures
         .filter(line -> line.split(",")[2].equals("wheel"))
         .mapToPair(line -> {
            String[] fields = line.split(",");
            return new Tuple2<String, Integer>(fields[1] + "_" + fields[0].split("/")[1], 1);
         })
         .reduceByKey((v1, v2) -> v1 + v2)
         .filter(pair -> pair._2() > 2)
         .map(pair -> pair._1().split("_")[1])
         .distinct();
      wheelFailuresPerMonth.saveAsTextFile(outputA);

      // B
      JavaRDD<String> bikeLT20Failures = failures
         .mapToPair(line -> {
            String[] fields = line.split(",");
            return new Tuple2<String, Integer>(fields[1], 1);
         })
         .reduceByKey((v1, v2) -> v1 + v2)
         .filter(pair -> pair._2() <= 20);
      JavaRDD<String> citiesLT20FailuresPerBike = bicycle
         .mapToPair(line -> {
            String[] fields = line.split(",");
            return new Tuple2<String, String>(fields[0], fields[2]);
         })
         .join(bikeLT20Failures)
         .map(pair -> pair._2()._1())
         .distinct();
      citiesLT20FailuresPerBike.saveAsTextFile(outputB);
   }
}