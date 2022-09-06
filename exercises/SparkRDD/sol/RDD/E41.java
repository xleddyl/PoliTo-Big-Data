package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Top-k most critical sensors
* @input: collection of structured csv files containing the daily value of PM10 for a set of sensors + the value of k
* @output: HDFS file containing the top-k critical sensors (> 50) (each line contains the number of days and the sensorId)
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];
      String k = Integer.parseInt(args[2]);

      SparkConf conf = new SparkConf().setAppName("E41");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> readings = sc.textFile(inputPath);
      JavaPairRDD<Integer, String> criticalSensors = readings
            .filter(line -> (new Double(line.split(",")[2]) > 50))
            .mapToPair(line -> {
               String[] fields = line.split(",");
               return (new Tuple2<String, Integer>(fields[0], new Integer(1)));
            })
            .reduceByKey((v1, v2) -> v1 + v2)
            .mapToPair(pair -> (new Tuple2<Integer, String>(pair._2(), pair._1())))
            .sortByKey(false);
      List<Tuple2<Integer, String>> topKCriticalSensors = criticalSensors.take(k);
      (sc.parallelizePairs(topKCriticalSensors)).saveAsTextFile(outputPath);

      //

      sc.close();
   }
}