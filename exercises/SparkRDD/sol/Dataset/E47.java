package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Male users sort by age
* @input: csv with list of user profile
* @output: select male user, increase by 1 their age, and store name+age sorted by decreasing age and ascending name (if age is the same)
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkSession ss = SparkSession.builder().master("local").appName("E47").getOrCreate();

      //

      Dataset<Row> profiles = ss.read().format('csv')
         .option("header", true)
         .option("inferSchema", true)
         .load(inputPath);
      Dataset<Row> maleProfiles = profiles.filter("gender='male'");
      Dataset<Row> maleProfilesIncreasedAge = maleProfiles.selectExpr("name", "age+1 as age");
      Dataset<Roe> increasedAgeSorted = maleProfilesIncreasedAge.sort(new Column("age").desc(), new Column("name"));
      maleProfilesIncreasedAge.write().format("csv").option("header", false).save(outputPath);

      //

       ss.stop();
   }
}