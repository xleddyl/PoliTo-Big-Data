package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Profile update
* @input1: file containing the list of movies watched by the users of a video on demand service (userid,movieid,start-timestamp,end-timestamp)
* @input2: file containing the list of preferences for each user (userid,movie-genre)
* @input3: file containing the list of movies with associated information (movieid,title,movie-genre)
* @output: select for each misleading profile the list of movies genres that are not in their preferred genres and are associated with at least 5 movies watched by the user
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath1 = args[0];
      String inputPath2 = args[2];
      String inputPath3 = args[3];

      SparkConf conf = new SparkConf().setAppName("E45");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> userWatchedMovies = sc.textFile(inputPath1);
      JavaRDD<String> userPreferences = sc.textFile(inputPath2);
      JavaRDD<String> moviesInfo = sc.textFile(inputPath3);

      JavaPairRDD<String, String> userPreferencesActualPAIR = FROM_E44;
      JavaPairRDD<String, String> userWatchedMoviesPAIR = FROM_E44;
      JavaPairRDD<String, String> moviesInfoPAIR = FROM_E44;

      JavaRDD<String> misleadingUsers = userWatchedMoviesNotLikedCount
               .join(userWatchedMoviesCount)
               .filter(pair -> {
                  int notLiked = pair._2().1();
                  int numWathced = pair._2().2();
                  return ((double) notLiked > (threshold *  (double) wathcedCount));
               })
      //

      sc.close();
   }
}