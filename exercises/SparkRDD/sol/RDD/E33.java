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

      SparkConf conf = new SparkConf().setAppName("E33");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> readings = sc.textFile(inputPath);
      JavaRDD<Double> PM10values = readings.map(line -> {
         String[] fields = line.split(",");
         return new Double(fields[2]);
      });
      List<Double> topPM10values = PM10values.top(3);
      for(Double value : topPM10Values) {
         System.out.println(value);
      }

      //

      sc.close();
   }
}