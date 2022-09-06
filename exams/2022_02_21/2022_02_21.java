// Q1 -> D
// Q2 -> B

// file structure 1: ItemID,Name,Category,FirstTimeInCatalog (ID1,t-shirt-winter,Clothing,2001/03/01-12:00:00) - ItemsCatalog
// file structure 2: Username,Name,Surname,DateOfBirth (User20,Paolo,Garza,1976/03/01) - Customers
// file structure 3: SaleTimestamp,Username,ItemID,SalePrice (2019/02/02-09:15:01,User20,ID1,50.99) - Purchases

// E1 -> MapReduce and Hadoop
// select the first year with max number of purchases -> (year, nPurchase)
class MapperEX extends Mapper< ... > {   // for file Purchases
   protected void map(LongWritable key, Text value, Context context) ... {
      String[] fields = value.toString().split(",");
      int year = Integer.parseInt(fields[0].split("/")[0]);
      context.write(new IntWritable(year), NullWritable.get());
   }
}

class ReducerEX extends Reducer< ... > {
   int maxYear;
   int maxSale;

   protected void setup(Context context) {
      maxYear = -1;
      maxSale = -1;
   }

   protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) {
      int sale = 0;
      for(NullWritable value : values) {
         sale ++;
      }
      if(sale > maxSale) {
         maxSale = sale;
         maxYear = key.get();
      } else if(sale == maxSale && key.get() < maxYear) {
         maxSale = sale;
         maxYear = key.get();
      }
   }

   protected void cleanup(Context context) {
      context.write(new IntWritable(maxYear), new IntWritable(maxSale));
   }
}

// E2 -> Spark and RDDs
// A -> select items sold >= 10k times in 2020 and >= 10k times in 2021 -> (ItemID)
// B -> (only elements included in catalog before 2020) select item if >= 2 months in 2020 such that in those months has < 10 distinct customers -> (ItemID, Category)
class SparkDriver {
   public static void main( ... ) ... {
      // init

      JavaRDD<String> itemsCatalog = sc.textFile(input1).cache();
      JavaPairRDD<String, String> purchases = sc.textFile(input2).cache();

      // A
      JavaPairRDD<String> sales2020and2021GT10k = purchases
         .filter(line -> {
            String[] fields = line.split(",");
            String year = fields[0].split("/")[0];
            return (year.equals("2021") || year.equals("2020"));
         })
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String ItemID = fields[2];
            String year = fields[0].split("/")[0];
            if(year.equals("2021")) {
               return new Tuple2<String, Counter>(ItemID, new Counter(1, 0));
            }
            return new Tuple2<String, Counter>(ItemID, new Counter(0, 1));
         })   // (ItemID, Counter)
         .reduceByKey((v1, v2) -> new Counter(v1.getSale2021() + v2.getSale2021(), v1.getSale2020() + v2.getSale2020()));
         .filter(pair -> {
            int sale2021 = pair._2().getSale2021();
            int sale2020 = pair._2().getSale2020();
            return (sale2021 >= 10000 && sale2020 >= 10000);
         })
         .map(pair -> pair._1());
      sales2020and2021GT10k.saveAsTextFile(outputA);

      // B
      JavaPairRDD<String, String> itemsIncludedBefore2020 = itemsCatalog
         .filter(line -> line.split(",")[3].split("-")[0].compareTo("2020/01/01") < 0)
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String ItemID = fields[0];
            String Category = fields[2];
            return new Tuple2<String, String>(ItemID, Category);
         });   // (ItemID, Category)
      JavaPairRDD<String, Integer> items2MonthsLT10distinctCustomers = purchases
         .filter(line -> line.split(",")[0].split("/")[0].equals("2020"))
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String Username = fields[1];
            String ItemID = fields[2];
            String month = fields[0].split("/")[1];
            return new Tuple2<String, Integer>(Username + "_" + ItemID + "_" + month, 1);
         })   // (Username_ItemID_month, 1)
         .distinct()   // distinct user per item per month
         .mapToPair(pair -> {
            String[] fields = pair._1().split("_");
            String ItemID = fields[1];
            String month = fields[2].split("/")[1];
            return new Tuple2<String, Integer>(ItemID + "_" + month, 1);
         })   // (ItemID_month, 1)
         .reduceByKey((v1, v2) -> v1 + v2)   // (ItemID_month, nSale)
         .filter(pair -> pair._2() < 10)
         .mapToPair(pair -> {
            String[] fields = pair._1().split("_");
            String ItemID = fields[0];
            return new Tuple2<String, Integer>(ItemID, 1);
         })   // (ItemID, 1)
         .reduceByKey((v1, v2) -> v1 + v2)   // (ItemID, nMonthLT10)
         .filter(pair -> pair._2() >= 2)
      JavaPairRDD<String, String> selectedItems = itemsIncludedBefore2020
         .join(items2MonthsLT10distinctCustomers)   // (ItemID, (Category, nMonthLT10))
         .mapToPair(pair -> new Tuple2<String, String>(pair._1(), pair._2()._1()));
      selectedItems.saveAsTextFile(outputB);
   }
}

class Counter {
   private int sale2021;
   private int sale2020;
}