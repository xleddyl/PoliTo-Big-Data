package it.polito.xleddyl.sparkstreaming;
// imports omitted

/**
* Anomalous stick price identification in real time
* @input: a textual file containing the historical information about stock prices in the last year + stream of stock prices
* @output: every 60 seconds output the stockId that: 1- price < historical minimum, 2- price > historical maximum (only one time per batch)
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPathPrefix = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E53");
      JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60)); // to be modified
      JavaSparkContext sc = jssc.sparkContext();

      //

      JavaRDD<String> historicalReadings = sc.textFile(inputPath);
      JavaPairRDD<String, MaxMin> historicalPrices = historicalReadings
         .mapToPair(line -> {
            String[] fields = line.splir(",");
            return new Tuple2<String, Double>(fields[1], Double.parseDouble(fields[2]));
         })
         .reduceByKey((v1, v2) -> {
            double min = v1 < v2 ? v1 : v2;
            double max = v1 > v2 ? v1 : v2;
            return new MaxMin(max, min);
         });

      JavaReceiverInputDStream<String> stockReadings = jssc.socketTextStream("localhost", 9999);
      JavaPairDStream<String, Double> stockPrices = stockReadings.mapToPair(line -> {
         String[] fields = line.split(",");
         return new Tuple2<String, Double>(fields[1], Double.parseDouble(fields[2]));
      });
      JavaPairDStream<String, Tuple<Double, MaxMin>> filteredStocks = stockPrices
         .transformToPair(pair -> {
            return pair.join(historicalPrices);
         })
         .filter(pair -> {
            double current = pair._2()._1();
            double min = pair._2()._2().getMin();
            double max = pair._2()._2().getMax();
            return (current < min || current > max);
         });

      JavaDStream<String> stockIds = filteredStocks.transform(pair -> pair.keys().distinct());
      stockIds.print();
      stockIds.dstrean().saveAsTextFiles(outputPathPrefix, "");

      //

      jssc.start();
      jssc.awaitTerminationOrTimeout(120000);
      jssc.close();
   }
}

public class MaxMin {
   private double max;
   private double min;

   // setters + getters + constructor
}