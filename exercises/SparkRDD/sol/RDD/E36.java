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

      SparkConf conf = new SparkConf().setAppName("E36");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> readings = sc.textFile(inputPath);
      JavaRDD<Double> PM10values = readings.map(line -> (new Double(line.split(",")[2]))).cache();
      Double sum = PM10values.reduce((v1, v2) -> new Double(v1 + v2));
      Double count = PM10value.count()
      System.out.println("avg: " + sum/count);

      //

      sc.close();
   }
}