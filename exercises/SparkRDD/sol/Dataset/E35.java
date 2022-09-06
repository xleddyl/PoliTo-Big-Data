package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Dates associated with the maximum value
* @input: collection of structured csv files containing the daily value of PM10 for a set of sensors
* @output: the dates associated with the maximum value of PM10
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkSession ss = new SparkSession.builder().appName("E35").getOrCreate();

      //

      Dataset<Row> readings = ss.read().format("csv")
         .option("header", false)
         .option("inferSchema", true)
         .load(inputPath);
      Double max = (Double) readings.agg(max("_c2")).first().getAs("max(_c2)");
      Dataset<Row> maxReadings = readings.filter("_c2="+max).select("_c1");
      maxReadings.write().format("csv").save(outputPath);

      //

      ss.stop();
   }
}