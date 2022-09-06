package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Top-k maximum values
* @input: collection of structured csv files containing the daily value of PM10 for a set of sensors
* @output: report the top-3 maximum values of PM10
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkSession ss = new SparkSession.builder().appName("E33").getOrCreate();

      //

      Dataset<Row> readings = ss.read().format("csv")
         .option("header", false)
         .option("inferSchema", true)
         .load(inputPath);
      List<Row> maxVals = readings.select("_c2")
         .sort(new Column("_c2").desc())
         .takeAsList(3);
      for(Row r : maxVals) {
         System.out.println((Double) r.getAs("_c2"));
      }

      //

      ss.stop();
   }
}