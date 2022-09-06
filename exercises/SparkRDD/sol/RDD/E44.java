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
      String outputPath = args[1];
      String inputPath1 = args[0];
      String inputPath2 = args[2];
      String inputPath3 = args[3];
      Double threshold = new Double(args[4]);

      SparkConf conf = new SparkConf().setAppName("E44");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> userWatchedMovies = sc.textFile(inputPath1);
      JavaRDD<String> userPreferences = sc.textFile(inputPath2);
      JavaRDD<String> moviesInfo = sc.textFile(inputPath3);

      // <movieid, userid>
      JavaPairRDD<String, String> userWatchedMoviesPAIR = userWatchedMovies.mapToPair(line -> {
         String[] fields = line.split(",");
         return (new Tuple2<String, String>(fields[1], fields[0]));
      });
      // <userid, movie-genre>
      JavaPairRDD<String, String> userPreferencesIdealPAIR = userPreferences.mapToPair(line -> {
         String[] fields = line.split(",");
         return (new Tuple2<String, String>(fields[0], fields[1]));
      });
      // <movieid, movie-genre>
      JavaPairRDD<String, String> moviesInfoPAIR = moviesInfo.mapToPair(line -> {
         String[] fields = line.split(",");
         return (new Tuple2<String, String>(fields[0], fields[2]));
      });
      // <movieid, <userid, movie-genre>>
      JavaPairRDD<String, Tuple2<String, String>> userWatchedMoviesJOIN = userWatchedMoviesPAIR.join(moviesInfoPAIR);

      // <userid, movie-genre>
      JavaPairRDD<String, String> userPreferencesActualPAIR = userWatchedMoviesJOIN.mapToPair(movieUserGenre -> {
         return (new Tuple2<String, String>(movieUserGenre._2()._1(), movieUserGenre._2()._2()));
      });

      // // <userid, <genreAct genreAct genreAct ..., genreIde genreIde genreIde ...>>
      // JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> userIdealActual = userPreferencesActualPAIR.cogroup(userPreferencesIdealPAIR);
      // JavaRDD<String> misleadingUsers = userIdealActual
      //          .filter(userListActIde -> {
      //             ArrayList<String> likedGenre = new ArrayList<String>();
      //             ArrayList<String> watchedGenre = new ArrayList<String>();
      //             userListActIde._2()._2().forEachRemaining(likedGenre::add);

      //             int wathcedCount = 0;
      //             int notLiked = 0;

      //             for(String watched : userListActIde._2()._1()) {
      //                wathcedCount ++;
      //                if(!likedGenre.contains(watched)) {
      //                   notLiked ++;
      //                }
      //             }

      //             return ((double) notLiked > (threshold *  (double) wathcedCount));
      //          })
      //          .keys();
      // misleadingUsers.saveAsTextFile(outputPath);

      // watched movies
      JavaPairRDD<String, Integer> userWatchedMoviesCount = userPreferencesActualPAIR.mapValues(v -> 1).reduce((v1, v2) -> v1 + v2);

      // wathced but not liked
      JavaPairRDD<String, String> userWatchedNotLiked = userPreferencesActualPAIR.subtract(userPreferencesIdealPAIR);

      // watched movies not liked
      JavaPairRDD<String, Integer> userWatchedMoviesNotLikedCount = userWatchedNotLiked.mapValues(v -> 1).reduce((v1, v2) -> v1 + v2);

      JavaRDD<String> misleadingUsers = userWatchedMoviesNotLikedCount
               .join(userWatchedMoviesCount)
               .filter(pair -> {
                  int notLiked = pair._2().1();
                  int numWathced = pair._2().2();
                  return ((double) notLiked > (threshold *  (double) wathcedCount));
               })
               .keys();
      misleadingUsers.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}