// Q1 -> B
// Q2 -> B

// file structure 1: BID,title,genre,publisher,yearOfpublication (B1020,The Body in the Library,Crime,Dodd and Company,1942)
// file structure 2: customerid,BID,date,price (customer1,BID20,20170502,19.99)

// E1 -> MapReduce and Hadoop
// select customerid who bought same book >= 2 times in 2018. output: customerid\tBID
class MapperEX extends Mapper< ... > {
   protected void map(LongWritable key, Text value, Context context) {
      String[] fields = value.toString().split(",");
      String date = fields[2];
      if(date.startsWith("2018")) {
         String customerid = fields[0];
         String BID = fields[0];
         context.write(new Text(customerid + "\t" + BID), NullWritable.get());
      }
   }
}

class ReducerEX extends Reducer< ... > {
   protected void reduce(Text key, Iterable<NullWritable> values, Context context) ... {
      if(values.size() >= 2) {
         context.write(key, NullWritable.get());
      }
   }
}

// E2 -> Spark and RDDs
// A -> compute maximum number of daily purchases for each book during 2018. output: BID,max
// B -> select for each book the window of three consecutive dates such that each date is characterized by > 10% of the purchases of the considered book in 2018 (n > 0.1 * tot)
class SparkDriver {
   public static void main( ... ) ... {
      // variables, sc, ss

      JavaRDD<String> books = sc.textFile(booksInput);
      JavaRDD<String> sale = sc.textFile(saleInput)
         .filter(line -> line.split(",")[2].startsWith("2018"));

      // A
      JavaPairRDD<String, Integer> bookMaxDaily = sale
         .mapToPair(line -> {
            String[] fields = line.split(",");
            return new Tuple2<String, Integer>(fields[1] + "_" + fields[2], 1);
         })
         .reduceByKey((v1, v2) -> v1 + v2)
         .mapToPair(pair -> {
            String[] fields = pair._1().split("_");
            return new Tuple2<String, Integer>(fields[0], pair._2());
         })
         .reduceByKey((v1, v2) -> v1 > v2 ? v1 : v2);
      bookMaxDaily.saveAsTextFile(outputA);

      // B
      JavaRDD<String, Integer> BIDTotPurchase = sale
         .mapToPair(line -> {
            String[] fields = line.split(",");
            new Tuple2<String, Integer>(fields[1], 1);
         })
         .reduceByKey((v1, v2) -> v1 + v2);
      JavaPairRDD<String, String> selectedWindows = sale
         .mapToPair(line -> {
            String[] fields = line.split(",");
            return new Tuple2<String, Integer>(fields[1] + "_" + fields[2], 1);
         })
         .reduceByKey((v1, v2) -> v1 + v2)
         .mapToPair(pair -> {
            String[] fields = pair._1().split("_");
            DatePurchase dp = new DatePurchase(Integer.parseInt(fields[1]), 1);
            return new Tuple2<String, Integer>(fields[0], dp);
         })
         .join(BIDTotPurchase)
         .filter(pair -> {
            int daily = pair._2()._1().getPurchase();
            int tot = pair._2()._2();
            return daili > 0.1 * tot;
         })
         .flatMapToPair(pair -> {
            ArrayList<Tuple2<String, Integer>> flatMap = new ArrayList<Tuple2<String, Integer>>();
            String BID = pair._1();
            int date = pair._2()._1().getDate();
            flatMap.add(new Tuple2<String, Integer>(BID + "," + date, 1));
            flatMap.add(new Tuple2<String, Integer>(BID + "," + (date - 1), 1));
            flatMap.add(new Tuple2<String, Integer>(BID + "," + (date - 2), 1));
            return flatMap.iterator();
         })
         .reduceByKey((v1, v2) -> v1 + v2)
         .filter(pair -> pair._2() == 3);
      selectedWindows.keys().saveAsTextFile(outputB);
   }
}

class DatePurchase {
   private int date;
   private int purchase;
   // setters, getters and constructor
}