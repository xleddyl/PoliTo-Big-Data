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

      SparkSession ss = new SparkSession.builder().appName("E38").getOrCreate();

      //

      Dataset<Row> readings = ss.read().format("csv")
         .input("header", false)
         .input("inferSchema", true)
         .load(inputPath);
      Dataset<Row> critical = radings.filter("_c2>50");
      RelationalGroupedDataset readingsPerSensor = critical.groupBy("_c0");
      Dataset<Row> countPerSensor = readingsPerSensor.count().withColumnRenamed("_c0", "sensorid");
      Dataset<Row> frequent = countPerSensor.filter("count>=2");
      frequent.write().format("csv").save(outputPath);

      //

      ss.stop();
   }
}