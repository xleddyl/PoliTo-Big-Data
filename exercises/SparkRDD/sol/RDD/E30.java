package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Log filtering
* @input: input a simple log of a web server (each line is associated with a URL request)
* @output: the lines containing the word "google", stored in an HDFS folder
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E30");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> logRDD = sc.textFile(inputPath);
      JavaRDD<String> googleRDD = logRDD.filter(logLine -> logLine.toLowerCase().contains("google"));
      googleRDD.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}
