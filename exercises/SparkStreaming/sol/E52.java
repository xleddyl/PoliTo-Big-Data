package it.polito.xleddyl.sparkstreaming;
// imports omitted

/**
* High stock price variation identification in real-time
* @input: stream of stock prices
* @output: every 30 seconds print stockID and price variation (%) (only if variation > 0.5%)
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPathPrefix = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E52");
      JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
      JavaSparkContext sc = jssc.sparkContext();

      //

      JavaReceiverInputDStream<String> prices = jssc.socketTextStream("localhost", 9999);
      JavaPairDStream<String, Double> stockIdPrice = prices.mapToPair(line -> {
         String[] fields = record.split(",");
         String stockID = fields[1];
         double price = Double.parseDouble(fields[2]);
         return (new Tuple2<String, Double>(stockID, price));
      });
      JavaPairDStream<String, MaxMin> stockIdMaxMin = stockIdPrice.reduceByKey((v1, v2)-> {
         double min = v1 < v2 ? v1 : v2;
         double max = v1 > v2 ? v1 : v2;
         return (new MaxMin(max, min));
      });
      JavaPairDStream<String, Double> stockIdVariation = stockIdMaxMin.filter(pair -> {
         return (pair._2().computeVariation() > 0.5);
      }).map(pair -> {
         return (new Tuple2<String, Double>(pair._1(), pair._2().computeVariation()));
      });
      stockIdVariation.print();
      stockIdVariation.dstream().saveAsTextFiles(outputPathPrefix, "");

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

   public double computeVariation() {
      return ((max - min) / max) * 100
   }
}