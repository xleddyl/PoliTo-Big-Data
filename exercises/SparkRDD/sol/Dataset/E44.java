package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Misleading profile selection
* @input1: file containing the list of movies watched by the users of a video on demand service (userid,movieid,start-timestamp,end-timestamp)
* @input2: file containing the list of preferences for each user (userid,movie-genre)
* @input3: file containing the list of movies with associated information (movieid,title,movie-genre)
* @output: select userId of the list with a misleading profice (more than T% of movies not associated with liked genre)
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[0];
      String inputPath1 = args[1];
      String inputPath2 = args[2];
      String inputPath3 = args[3];

      SparkSession ss = new SparkSession.builder().appName("E44").getOrCreate();

      //

      Dataset<Row> watched = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath1);
      Dataset<Row> preferences = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath2);
      Dataset<Row> moviesInfo = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath3);

      Dataset<Row> userWatchedGenre = watched.join(moviesInfo, wathced.col("movieId").equalTo(moviesInfo.col("movieId")))
         .select("userId", "movieGenre");
      Dataset<Row> userWatchedLiked = userWatchedGenre.join(preferences, JavaConversions.asScalaBuffer(asList("userId", "movieGenre")), "leftouter");
      ss.udf().register("notLikedF", (Integer value) -> {
         return value == null ? 1 : 0;
      }, DataTypes.IntegerType);
      Dataset<Row> userWatched = userWatchedLiked.selectExpr("userId", "movieGenre", "notLikedF(constant) as notLiked");
      Dataset<Row> userPercNotLiked = userWatched.groupBy("userId").agg(avg("notLiked")).withColumnRenamed("avg(notLiked)", "percNotLiked");
      Dataset<Row> misleadingUser = userPercNotLiked.filter("percNotLiked>"+threshold).select("userId");
      misleadingUser.write().format("csv").option("header", false).save(outputPath);

      //

      ss.stop();
   }
}