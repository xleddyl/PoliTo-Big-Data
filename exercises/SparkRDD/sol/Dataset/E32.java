package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Maximum value
* @input: collection of structured csv files containing the daily value of PM10 for a set of sensors
* @output: report the maximum value of PM10 (printed on std out)
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkSession ss = new SparkSession.builder().appName("E32").getOrCreate();

      //

      Dataset<Row> readings = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath);
      Dataset<Row> maxReadings = readings.agg(max("_c2"));
      Double maxVal = (Double) maxReadings.first().getAs("max(_c2)");
      System.out.println(maxVal);

      //

      ss.stop();
   }
}