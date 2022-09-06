package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* TITLE
* @input
* @output:
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath1 = args[0];
      String inputPath2 = args[2];
      String inputPath3 = args[3];

      SparkConf conf = new SparkConf().setAppName("E43");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //



      //

      sc.close();
   }
}