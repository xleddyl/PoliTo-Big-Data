package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Order sensors by number of critical days
* @input: collection of structured csv files containing the daily value of PM10 for a set of sensors
* @output: sensors ordered by the number of critical days (> 50)
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E40");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> readings = sc.textFile(inputPath);
      JavaPairRDD<Integer, String> criticalDays = readings
               .filter(line -> (new Double(line.split(",")) > 50))
               .mapToPair(line -> {
                  String[] fields = line.split(",");
                  return (new Tuple2<String, Integer>(fields[0], new Integer(1)));
               })
               .reduceByKey((v1, v2) -> v1 + v2)
               .mapToPair(pair -> (new Tuple2<Integer, String>(pair._2(), pair._1())))
               .sortByKey(false);
      criticalDays.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}