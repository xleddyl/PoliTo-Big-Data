package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Time series analysis
* @input: textual file containing a set of temperature readings (sample rate is 1 minute)
* @output: consider all winfows of 3 consecutive readings and select the windows with an increasing trend
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath = args[0];

      SparkConf conf = new SparkConf().setAppName("E46");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaPairRDD<Integer, Tuple2<Integer, Double>> readings = sc.textFile(inputPath)
         .flatMapToPair(line -> {
            String[] fields = line.split(",");
            ArrayList<Tuple2<Integer, Tuple2<Integer, Double>>> pairs = new ArrayList<Tuple2<Integer, Tuple2<Integer, Double>>>();
            pairs.add(new Tuple2<Integer, Tuple2<Integer, Double>>(new Integer(fields[0]), new Tuple2<Integer, Double>(new Integer(fields[0]), new Double(fields[1]))));
            pairs.add(new Tuple2<Integer, Tuple2<Integer, Double>>(new Integer(fields[0]) - 60, new Tuple2<Integer, Double>(new Integer(fields[0]), new Double(fields[1]))));
            pairs.add(new Tuple2<Integer, Tuple2<Integer, Double>>(new Integer(fields[0]) - 120, new Tuple2<Integer, Double>(new Integer(fields[0]), new Double(fields[1]))));
            return pairs.iterator();
         });
      JavaRDD<Iterable<Tuple2<Integer, Double>>> timestampsWindows = readinsg.groupByKey()
         .values()
         .filter(list -> {
            HashMap<Integer, Double> timestampTemp = new HashMap<Integer, Double>();
            int minTimestamp = Integer.MAX_VALUE;
            for(Tuple2<Integer, Double> element : list) {
               timestampTemp.put(element._1(), element._2());
               if(element._1() < minTimestamp) {
                  minTimestamp = element._1();
               }
            }

            boolean increasing = true;
            if(timestampTemp.size() == 3) {
               for(int i = minTimestamp + 60; ts <= minTimestamp + 120 && increasing; ts += 60) {
                  if(timestampTemp.get(ts) <= timestampTemp.get(ts - 60)) {
                     increasing = false;
                  }
               }
            } else {
               increasing = false;
            }

            return increasing;
         });
         timestampsWindows.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}