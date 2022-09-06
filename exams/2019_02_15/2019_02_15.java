// Q1 -> D
// Q2 -> C

// E1 -> MapReduce and Hadoop
// select italian cities with >= 1000 POI category = tourism, in which >= 20 subcategory = museum
class MapperEX extends Mapper<LongWritable, Text, Text, Text> {
   protected void map(LongWritable key, Text value, Context context) ... {
      String[] fields = value.toString().split(",");
      if(fields[4].equals("Italy") && fields[5].equals("tourism")) {
         context.write(new Text(fields[3]), new IntWritable(0));
         if(fields[5].equals("museum")) {
            context.write(new Text(fields[3]), new IntWritable(1));
         }
      }
   }
}

class ReducerEX extends Reducer<Text, IntWritable, Text, NullWritable> {
   protected void reduce(Text key, Iterable<IntWritable> values, Context context) ... {
      int poi = 0;
      int museums = 0;
      for(IntWritable value : values) {
         poi++;
         museums += value.get();
      }
      if(poi > 1000 && museums >= 20) {
         context.write(key, NullWritable.get());
      }
   }
}

// E2 -> Spark and RDDs
// A -> select cities with at least one taxi but without busstop
// B -> select italian cities with museum > avg museum per city in italy, one city per line
public class SparkDriver {
   public static void main( ... ) ... {
      // variables
      // SparkSession ss
      // JavaSparkContext sc

      Dataset<Row> pois = ss.read().format("csv")
         .option("header", false)
         .option("inferSchema", false)
         .load(inputFile)
         .filter("_c4=Italy");

      // A
      Dataset<Row> citiesA = pois
         .filter("_c6=Taxi and _c6!=Busstop")
         .groupBy("_c3")
         .select("_c3");
      citiesA.write().format("csv").option("header", false).save(ouptupA);

      // B
      Double avg = (Double) pois
         .filter("_c6=Museum")
         .groupBy("_c3")
         .count()
         .agg(avg("count"))
         .getAs("avg(count)");
      Dataset<Row> citiesB = pois
         .groupBy("_c3")
         .count()
         .filter("count>" + avg)
         .select("_c3");
      citiesB.write().format("csv").option("header", false).save(ouptupA);
   }
}