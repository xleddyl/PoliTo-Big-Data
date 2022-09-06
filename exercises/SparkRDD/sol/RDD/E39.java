package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Critical dates analysis
* @input: collection of structured csv files containing the daily value of PM10 for a set of sensors
* @output: one line per sensor containing the list of dates with a PM10 value > 50 for that sensor
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E39");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> readings = sc.textFile(inputPath);
      JavaPairRDD<String, Iterable<String>> sensorCriticalDates = readings
            .filter(line -> (Double.parseDouble(line.split(",")) > 50))
            .mapToPair(line -> {
               String[] fields = liine.split(",");
               return (new Tuple2<String, String>(fields[0], fields[1]));
            })
            .groupByKey();
      sensorCriticalDates.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}