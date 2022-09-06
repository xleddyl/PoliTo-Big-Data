// Q1 -> D
// Q2 -> C

// file structure 1: Username,Gender,YearOfBirth,Country (PaoloG76,Male,1976,Italy)
// file structure 2: MID,Title,Director,ReleaseDate (MID124,Ghostbusters,Ivan Reitman,1984/05/01)
// file structure 3: Username,MID,StartTimestamp,EndTimestamp (PaoloG76,MID124,2010/06/01_14:18,2010/06/01_16:10)

// E1 -> MapReduce and Hadoop
// select movies watched by one single user in 2019. output: one MID per line
class MapperEX extends Mapper< ... > { // for file 3
   protected void map(LongWritable key, Text value, Context context) ... {
      String[] fields = value.toString().split(",");
      if(fields[2].split("/")[0].equals("2019")) {
         context.write(new Text(fields[1]), new Text(fields[0]));
      }
   }
}

class ReducerEX extends Reducer< ... > {
   protected void reduce(Text key, Iterable<Text> values, Context context) ... {
      String prev = null;
      Boolean diff = false;
      for(Text value : values) {
         if(prev != null && prev.equals(value.toString())) {
            diff = true;
            break;
         }
         prev = value;
      }
      if(!diff) {
         context.write(key, NullWritable.get());
      }
   }
}


// E2 -> Spark and RDDs
// A -> select movies that have been watched only in one year from 2015 to 2020 and at least 1000 times in that year. output: MID,year
// B -> select movies most popular in at least two years (distinct users that watched that movie)
class SparkDriver {
   public static void main( ... ) ... {
      // init

      JavaRDD<String> users = sc.textFile(usersInput);
      JavaRDD<String> movies = sc.textFile(moviesInput);
      JavaRDD<String> watchedMovies = sc.textFile(watchedMoviesInput);

      // A
      JavaPairRDD<String, Integer> moviesWatchedCount = watchedMovies
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String MID = fields[1];
            String year = fields[2].split("/")[0];
            return new Tuple2<String, Integer>(MID + "_" + year, 1);
         })   // (MID_YEAR, 1)
         .reduceByKey((v1, v2) -> v1 + v2)
         .filter(pair -> pair._2() >= 1000)
         .mapToPair(pair -> new Tuple2<String, String>(pair._1().split("_")[0], pair._1().split("_")[1]))   // (MID, YEAR)
      JavaPairRDD<String, String> pointA = watchedMovies
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String MID = fields[1];
            Integer year = Integer.parseInt(fields[2].split("/")[0]);
            return new Tuple2<String, Integer>(MID, year);
         })   // (MID, YEAR)
         .filter(pair -> (2015 <= pair._2() <= 2020))   // year range 2015-2020
         .distinct()   // remove duplicate pairs
         .mapValues(year -> 1)
         .reduceByKey((v1, v2) -> v1 + v2)   // count number of distinct years
         .filter(pair -> pair._2() == 1)   // only movies watched 1 single year
         .join(moviesWatchedCount) // (MID, (DISTINCT YEAR COUNT, YEAR))
         .mapToPair(pair -> new Tuple2<String, String>(pair._1(), pair._2()._2()));   // (MID, YEAR)
      pointA.saveAsTextFile(outputA);

      // B
      JavaPairRDD<String, String> MIDYearWatch = watchedMovies
         .map(line -> {
            String[] fields = line.split(",");
            String MID = fields[1];
            String username = fields[0];
            String year = fields[2].split("/")[0];
            return (MID + "_" + username + "_" + year);
         })   // MID_USERNAME_YEAR
         .distinct()
         .mapToPair(line -> {
            String[] fields = line.split("_");
            String MID = fields[0];
            String year = fields[2];
            return new Tuple2<String, Integer>(MID + "_" + year, 1);
         })   // (MID_YEAR, 1)
         .reduceByKey((v1, v2) -> v1 + v2);   // (MID_YEAR, WATCH COUNT)
      JavaPairRDD<String> yearMaxWatch = MIDYearWatch
         .mapToPair(pair -> {
            String[] fields = pair._1().split(",");
            String year = fields[1];
            return new Tuple2<String, Integer>(year, pair._2());
         })   // (YEAR, WATCH COUNT)
         .reduceByKey((v1, v2) -> (v1 > v2 ? v1 : v2))   // (YEAR, MAX WATCH COUNT)
      JavaPairRDD<String, String> pointB = MIDYearWatch
         .mapToPair(pair -> {
            String[] fields = pair._1().split(",");
            String MID = fields[0];
            String year = fields[1];
            return new Tuple2<String, MIDWatched>(year, new MIDWatched(MID, pair._2()));
         })   // (YEAR, (MID, WATCH COUNT))
         .join(yearMaxWatch)   // (YEAR, ((MID, WATCH COUNT), MAX WATCH COUNT))
         .filter(pair -> {
            Integer MIDWatch = pair._2()._1().getCount();
            Integer yearMaxWatch = pair._2()._2();
            return MIDWatch == yearMaxWatch;
         })
         .mapToPair(pair -> {
            String MID = pair._2()._1().getMID();
            return new Tuple2<String, Integer>(MID, 1);
         })   // (MID, 1)
         .reduceByKey((v1, v2) -> v1 + v2)
         .filter(pair -> pair._2() >= 2)
         .map(pair -> pair._1());   // MID
      pointB.saveAsTextFile(outputB);
   }
}

class MIDWatched {
   private String MID,
   private Integer count

   // setters and getters
}