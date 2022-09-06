package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Recurring names
* @input: csv with list of user profile
* @output: select the names occurring at least two times and store name+avg(age) of selected names
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkSession ss = SparkSession.builder().master("local").appName("E48").getOrCreate();

      //

      Dataset<Row> profiles = ss.read().format("cvs")
         .option("header", true)
         .option("inferSchema", true)
         .load(inputPath);
      Dataset<Row> nameAvgAgeCount = profiles.groupBy("name")
         .agg(avg("age"), count("*"))
         .withColumnRenamed("avg(age)", "avgage")
         .withColumnRenamed("count(1)", "count")
         .filter("count>=2")
         .select("name", "avgage")
      nameAvgAgeCount.write().format("csv").option("header", false).save(outputPath);

      //

      ss.stop();
   }
}