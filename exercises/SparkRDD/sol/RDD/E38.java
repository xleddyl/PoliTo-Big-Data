package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Pollution analysis
* @input: collection of structured csv files containing the daily value of PM10 for a set of sensors
* @output: the sensor with at least 2 readings with a PM10 value greater than 50 + the number of times
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E38");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> readings = sc.textFile(inputPath);
      // JavaPairRDD<String, Integer> overThreshold = readings
      //       .filter(line -> ((new Double(line.split(",")[2]) > 50)))
      //       .mapToPair(line -> (new Tuple2<String, Integer>(new String(line.split(",")[0]), new Integer(1))))
      //       .reduceByKey((v1, v2) -> v1 + v2)
      //       .filter(pair -> (pair._2().intValue() >= 2));
      JavaPairRDD<String, Integer> overThreshold = readings
            .flatMapToPair(line -> {
               String[] fields = line.split(",");
               ArrayList<Tuple2<String, Integer>> sensorOne = new ArrayList<Tuple2<String, Integer>>();
               if(Double.parseDouble(fields[2]) > 50) {
                  sensorOne.add(new Tuple<String, Integer>(fields[0], 1));
               }
               return sensorOne.iterator();
            })
            .reduceByKey((v1, v2) -> v1 + v2)
            .filter(pair -> (pair._2().intValue() >= 2));
      overThreshold.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}