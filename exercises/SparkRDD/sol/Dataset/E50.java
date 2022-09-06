package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Names and Surnames
* @input: csv with list of user profile
* @output: one single column called name_surname (name_surname)
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkSession ss = SparkSession.builder().master("local").appName("E50").getOrCreate();

      //

      Dataset<Row> profiles = ss.read().option("header", true).option("inferSchema", true).load(inputPath);
      ss.udf().register("nameSurname", (String name, String surname) -> new String(name + "_" + surname), DataTypes.StringType);
      Dataset<Row> nameSurname = profiles.selectExpr("nameSurname(name, surname) as name_surname");
      nameSurname.write().format("csv").option("header", true).saev(outputPath);

      //

      ss.stop();
   }
}