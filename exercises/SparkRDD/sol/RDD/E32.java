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

      SparkConf conf = new SparkConf().setAppName("E32");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> readings = sc.textFile(inputPath);
      JavaRDD<Double> PM10values = readings.map(line -> {
         String[] fields = line.split("");
         return new Double(fields[2]);
      });
      Double topPM10value = PM10values.reduce((v1, v2) -> {
         return (v1 > v2 ? v1 : v2);
      });
      // or Double topPM10value = PM10values.top(1);
      System.out.println(topPM10value);

      //

      sc.close();
   }
}