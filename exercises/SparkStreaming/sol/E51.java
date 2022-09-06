package it.polito.xleddyl.sparkstreaming;
// imports omitted

/**
* Full station identification in real-time
* @input: textual file containing the list of stations of a bike sharing system + a stream of readings about the status of the stations
* @output: for each reading with 0 slots print the name every two seconds
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPathPrefix = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E51");
      JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
      JavaSparkContext sc = jssc.sparkContext();

      //

      JavaPairRDD<String, String> stationName = sc.textFile(inputPath).mapToPair(line -> {
         String[] fields = line.split(",");
         return (new Tuple2<String, String>(fields[0], fields[3]));
      });
      JavaReceiverInputStream<String> readings = jssc.socketTextStream("localhost", 9999);

      JavaDStream<String> fullStations = readings.filter(line -> {
         return (Integer.parseInt(line.split(",")[1]) == 0);
      })

      JavaPairDStream<String, String> stationNameTime = readings.mapToPair(pair -> {
         JavaPairRDD<String, Tuple2<String, String>> stationIdTimeName = pair.join(stationName);
         return stationIdTimeName.values();
      })

      stationNameTime.print();
      stationNameTime.dstream().saveAsTextFiles(outputPathPrefix, "");

      //

      jssc.start();
      jssc.awaitTerminationOrTimeout(120000);
      jssc.close();
   }
}