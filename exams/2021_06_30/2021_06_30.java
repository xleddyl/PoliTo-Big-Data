// Q1 -> A
// Q2 -> C

// file structure 1: ModelID,MName,Manufacturer (Model10,CBR125,Honda) - BikeModels
// file structure 2: SID,BikeID,ModelID,Date,Country,Price,EU (SID10,BikeIDFT2011,Model10,1985/05/02,Italy,5900,T) - Sales

// E1 -> MapReduce and Hadoop
// select ModelID of motorbike models sold >= 10000 times in EU=T and >= 10000 times in EU=F during 2020 -> (ModelID)
class MapperEX extends Mapper< ... > { // on file Sales
   protected void map(LongWritable key, Text value, Context context) ... {
      String[] fields = value.toString().split(",");
      String year = fields[3].split("/")[0];
      String ModelID = fields[2];
      String EU = fields[6];
      if(year.equals("2020")) {
         context.write(new Text(ModelID), new Text(EU));
      }
   }
}

class ReducerEX extends Reducer< ... > {
   protected void reduce(Text key, Iterable<Text> values, Context context) ... {
      int t = 0;
      int f = 0;
      for(Text value : values) {
         if(value.toString().equals("T")) {
            t ++;
         } else {
            f ++;
         }
      }
      if(t >= 10000 && f >= 10000) {
         context.write(key, NullWritable.get());
      }
   }
}

// E2 -> Spark and RDDs
// A -> (EU=T / Date=2020) select ModelID with price variation >= 5000 (max price 2020 - min price 2020) -> (ModelID)
// B -> select Manufacturer with >= 15 ModelID unsold or infrequently sold (0<= s <=10) -> (Manufacturer, nTot)
class SparkDriver {
   public static main( ... ) ... {
      // init

      JavaPairRDD<String, String> bikeModels = sc.textFile(input1)
         .mapToPair(line -> {
            String[] fields = line.split(",");
            return new Tuple2<String, String>(fields[0], fields[1]);
         })   // (ModelID, Manufacturer)
         .cache();
      JavaRDD<String> sales = sc.textFile(input2)
         .cache();

      // A
      JavaRDD<String, Double> bikePrices = sales
         .filter(line -> {
            String[] fields = line.split(",");
            String year = fields[3].split("/")[0];
            String EU = fields[6];
            return (year.equals("2020") && EU.equals("T"));
         })
         .mapToPair(line -> {
            String[] fields = line.split(",");
            Double Price = Double.parseDouble(fields[5]);
            String ModelID = fields[2];
            return new Tuple2<String, String>(ModelID, Price);
         });   // (ModelID, Price)
      JavaRDD<String, Double> maxPrices = bikePrices
         .reduce((v1, v2) -> (v1 > v2 ? v1 : v2));   // (ModelID, maxPrice)
      JavaRDD<String, Double> minPrices = bikePrices
         .reduce((v1, v2) -> (v1 > v2 ? v2 : v1));   // (ModelID, minPrice)
      JavaRDD<String, Double> priceVariation = maxPrices
         .join(minPrice)   // (ModelID, (maxPrice, minPrice))
         .mapValues(pair -> pair._2() - pair._1())
         .filter(pair -> (pair._2() >= 5000));
      priceVariation.keys().saveAsTextFile(outputA);

      // B
      JavaPairRDD<String, Integer> bikePerSell = sales
         .mapToPair(line -> {
            String[] fields = line.split(",");
            String ModelID = fields[2];
            return new Tuple2<String, Integer>(ModelID, 1);
         })
         .reduceByKey((v1, v2) -> v1 + v2);   // (ModelID, sell count)
      JavaPairRDD<String, Integer> bikeNotSellOrInfrequent = bikeModels
         .mapToPair(pair -> new Tuple2<String, Integer>(pair._1(), 0))
         .subtractByKey(bikePerSell)
         .union(bikePerSell)
         .filter(pair -> pair._2() <= 10);
      JavaPairRDD<String, Integer> manufacturerInfrequentCount = bikeModels
         .join(bikeNotSellOrInfrequent)   // (ModelID, (sell count, Manufacturer))
         .mapToPair(pair -> new Tuple2<String, Integer>(pair._2()._2(), 1))
         .reduceByKey((v1, v2) -> v1 + v2);
         .filter(pair -> pair._2() >= 15);
      manufacturerInfrequentCount.saveAsTextFile(outputB)
   }
}