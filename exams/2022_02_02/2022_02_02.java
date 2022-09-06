// Q1 -> B
// Q2 -> D

// file structure 1: AppId,AppName,Price,Category,Company (App10,PolitoApp,0,Education,Polito) - Apps
// file structure 2: UserId,Name,Surname (User15,Paolo,Garza) - Users
// file structure 3: UserId,AppId,Timestamp,Action (User15,App10,2019/01/01-23:01:15,Install) - Actions

// E1 -> MapReduce and Hadoop
// (Category = Game) select companies that developed more free apps than paid ones -> (Company, nTotGames)
class MapperEX extends Mapper< ... > { // for file Apps
   protected void map(LongWritable key, Text value, Context context) ... {
      String[] fields = value.toString().split(",");
      String Category = fields[3];
      if(Category.equals("Game")) {
         String Company = fields[4];
         int Price = Integer.parseInt(fields[2]) == 0 ? 1 : -1;
         context.write(new Text(Company), new IntWritable(Price));
      }
   }
}

class ReducerEX extends Reducer< ... > {
   protected void reduce(Text key, Iterable<IntWritable> values, Context context) ... {
      int totApp = 0;
      int diff = 0;
      for(IntWritable value : values) {
         diff += value.get();
         totApp++;
      }
      if(diff > 0) {
         context.write(key, new IntWritable(totApp));
      }
   }
}


// E2 -> Spark and RDDs
// A -> (2021) select AppId and AppName with montly Install > monthly Remove (for every of the 12 months) -> (AppId, AppName)
// B -> select AppId with max number of distinct new user after 2021/12/31 -> (AppId)
class SparkDriver {
   public static void main( ... ) ... {
      // init

      JavaPairRDD<String, String> apps = sc.textFile(input1)
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String AppId = fields[0];
            String AppName = fields[1];
            return new Tuple2<String, String>(AppId, AppName);
         })   // (AppId, AppName)
         .cache();
      JavaRDD<String> actions = sc.textFile(input3)
         .cache();

      // A
      JavaPairRDD<String, Integer> appIdInstallGTRemove12Months = actions
         .filter(line -> {
            String[] fields = line.split(",");
            String Action = fields[3];
            String year = fields[2].split("/")[0];
            return (year.equals("2021") && (Action.equals("Install") || Action.equals("Remove")));
         })
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String AppId = fields[0];
            String Action = fields[3];
            String Month = fields[2].split("/")[1];
            int value = Action.equals("Install") ? 1 : -1;
            return new Tuple2<String, Integer>(AppId + "_" + Month, value);
         })   // (AppId_Month, +-1)
         .reduceByKey((v1, v2) -> v1 + v2)   // (AppId_Month, diffInstallRemove)
         .filter(pair -> pair._2() > 0);
         .mapToPair(pair -> {
            String[] fields = pair._1().split("_");
            String AppId = fields[0];
            return new Tuple2<String, Integer>(AppId, 1);
         })   // (AppId, 1)
         .reduceByKey((v1, v2) -> v1 + v2)
         .filter(pair -> pair._2() == 12);   // (AppId, 12)

       JavaPairRDD<String, String> appIdAppNameInstallGTRemove12Months = apps
         .join(appIdInstallGTRemove12Months)   // (AppId, (AppName, 12))
         .mapToPair(pair -> new Tuple2<String, String>(pair._1(), pair._2()._1()));
      appIdAppNameInstallGTRemove12Months.saveAsTextFile(outputA);

      // B
      JavaRDD<String, Integer> appNewUsersAfterDate = actions
         .filter(line -> line.split(",")[3].equals("Install"))
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String UserId = fields[0];
            String AppId = fields[1];
            String date = Integer.parseInt(fields[2].split("-")[0];
            if(date.compareTo("2021/12/31") > 0) {
               return new Tuple2<String, Counter>(AppId + "_" + UserId, new Counter(1, 0));
            } else {
               return new Tuple2<String, Counter>(AppId + "_" + UserId, new Counter(0, 1));
            }
         })   // (AppId_UserId, Counter)
         .reduceByKey((v1, v2) -> new Counter(v1.getAfter() + v2.getAfter(), v1.getBefore() + v2.getBefore()))
         .filter(pair -> (pair._2().getAfter() > 0 && pair._2().getBefore() == 0))   // filter user installed only after date
         .mapToPair(pair -> {
            String[] fields = pair._1().split("_");
            String AppId = fields[0];
            String UserId = fields[1];
            return new Tuple2<String, String>(AppId, 1);
         })   // (AppId, 1)
         .reduceByKey((v1, v2) -> v1 + v2)   // number of installations per app
         .cache();
      int maxNewUserAfterDate = appNewUsersAfterDate.values().reduce((v1, v2) -> v1 > v2 ? v1 : v2);
      JavaRDD<String> appMaxNewUsersAfterDate = appNewUsersAfterDate
         .filter(pair -> pair._2() == maxNewUserAfterDate)   // app with max number of installation
         .keys();
      appMaxNewUsersAfterDate.saveAsTextFile(outputB);
   }
}

class Counter{
   private int after;
   private int before;
}