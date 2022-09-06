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

      SparkSession ss = new SparkSession.builder().appName("E37").getOrCreate();

      //

      Dataset<Row> readings = ss.read().format("csv")
         .option("header", false)
         .option("inferSchema", true)
         .load(inputPath);
      RelationalGroupedDataset readingsPerSensor = readings.groupBy("_c0");
      Dataset<Row> maxValPerSensor = readingsPerSensor.max("_c2");
      maxValPerSensor.write().format("csv").save(outputPath);

      //

      ss.stop();
   }
}