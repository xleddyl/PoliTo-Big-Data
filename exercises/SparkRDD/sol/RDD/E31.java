package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Log analysis
* @input: log of a web server (each line is a requested URL)
* @output: list of distinct IP addresses associated with the connections to a google page
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E31");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> logRDD = sc.textFile(inputPath);
      JavaRDD<String> googleRDD = logRDD.filter(log -> log.toLowerCase().contains("google"));
      JavaRDD<String> ipRDD = googleRDD.map(log -> {
         String[] fields = log.split(" ");
         return fields[0];
      }).distinct();
      ipRDD.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}