package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Readings associated with the maximum value
* @input: collection of structured csv files containing the daily value of PM10 for a set of sensors
* @output: the lines associated with the maximum value of PM10
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E34");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> readings = sc.textFile(inputPath).cache(); // cache because used more than 1 time
      Double maxPM10 = readings.map(line -> {
         String[] fields = line.split(",");
         return new Double(fields[2]);
      }).reduce((v1, v2) -> (v1 > v2 ? v1 : v2));
      JavaRDD<String> maxReadings = readings.filter((line -> {
         Double PM10 = new Double(line.split(",")[2]);
         return PM10.equals(maxPM10);
      }));
      maxReadings.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}