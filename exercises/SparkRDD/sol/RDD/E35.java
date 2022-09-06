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

      SparkConf conf = new SparkConf().setAppName("E35");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> readings = sc.textFile(inputPath).cache();
      Double maxPM10 = readings.map(line -> (new Double(line.split(",")[2]))).top(1);
      JavaRDD<Strign> dates = readings
         .filter(line -> ((new Double(line.split(",")[2])).equals(maxPM10)))
         .map(line -> new String(line.split(",")[1]))
         .distinct();
      dates.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}