package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Average value
* @input: collection of structured csv files containing the daily value of PM10 for a set of sensors
* @output: compute the average PM10 value
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkSession ss = new SparkSession.builder().appName("E36").getOrCreate();

      //

      Dataset<Row> readings = ss.read().format("csv")
         .option("header", false)
         .option("inferSchema", true)
         .load(inputPath);
      Double avg = (Double) readings.agg(avg("_c2")).first().getAs("avg(_c2)");
      System.out.println(avg);

      //

      ss.stop();
   }
}