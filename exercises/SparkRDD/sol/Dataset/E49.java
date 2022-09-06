package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Range Age
* @input: csv with list of user profile
* @output: one line per profile where the original age is substituted with a new attribute called rangeage ([(age/10)*10 - (age/10)*10+9])
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkSession ss = SparkSession.builder().master("local").appName("E49").getOrCreate();

      //

      Dataset<Row> profiles = ss.read().format("csv")
         .option("header", true).option("inferSchema", true)
         .load(inputPath);

      ss.udf().register("rangeAge", (Integer age) -> {
         int min = (age / 10) * 10;
         int max = min + 9;
         return new String("[" + min + "-" + max + "]");
      }, DataTypes.StringType);

      Dataset<Row> profileRangeage = profiles.selectExpr("name", "surname", "rangeAge(age) as rangeage");
      profileRangeage.write().format("csv").option("header", true).save(outputPath);

      //

      ss.stop();
   }
}