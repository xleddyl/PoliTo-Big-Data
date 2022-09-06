package it.polito.xleddyl.sparkrdd;
// imports omitted

/**
* Mapping QA
* @input: large textual file with set of questions + large textual file with set of answers
* @output: a file containing one line for each question and its list of answers
*/

// SPARK DRIVER
public class SparkDriver {
   public static void main(String args[]) throws Exception {
      String outputPath = args[1];
      String inputPath1 = args[0];
      String inputPath2 = args[2];

      SparkConf conf = new SparkConf().setAppName("E42");
      JavaSparkContext sc = new JavaSparkContext(conf);

      //

      JavaRDD<String> questions = sc.textFile(inputPath1);
      JavaRDD<String> answers = sc.textFile(inputPath2);
      JavaPairRDD<String, String> questionsCode = questions.mapToPair(line -> {
         String[] fields = line.split(",");
         return (new Tuple2<String, String>(fields[0], fields[2]));
      });
      JavaPairRDD<String, String> answersCode = questions.mapToPair(line -> {
         String[] fields = line.split(",");
         return (new Tuple2<String, String>(fields[1], fields[3]));
      });
      JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> answeredQuestions = questionsCode.cogroup(answersCode);
      answeredQuestions.saveAsTextFile(outputPath);

      //

      sc.close();
   }
}