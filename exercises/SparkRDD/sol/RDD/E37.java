package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Maximum values
* @input: collection of structured csv files containing the daily value of PM10 for a set of sensors
* @output: maximum value for each sensor
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E37");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> readings = sc.textFile(inputPath);
      JavaPairRDD<String, Double> sensorMax = readings
            .mapToPair(line -> {
               String[] fields = line.split(",");
               Tuple2<String, Double> pair = new Tuple2<String, Double>(new String(fields[0]), new Double(fields[2]));
               return pair;
            })
            .reduceByKey((v1, v2) -> (v1 > v2 ? v1 : v2));
      sensorMax.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}