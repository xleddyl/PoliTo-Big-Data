# RDD-based programming

- [SparkContext](#sparkcontext)
- [RDDs creation](#rdds-creation)
- [Save RDDs](#save-rdds)
- [RDDs operations](#rdds-operations)
- [Basic RDD Transformations](#basic-rdd-transformations)
- [Basic RDD Actions](#basic-rdd-actions)
- [Pair RDDs](#pair-rdds)
- [Transformations on Pair RDDs](#transformations-on-pair-rdds)
- [Transformations on pairs of Pair RDDs](#transformations-on-pairs-of-pair-rdds)
- [Actions on Pair RDDs](#actions-on-pair-rdds)
- [Double RDDs](#double-rdds)
- [Persistence and Cache](#persistence-and-cache)
- [Accumulators](#accumulators)
- [Broadcast variables](#broadcast-variables)

## SparkContext

JavaSparkContext build with the constructor, the only param is a configuration object.

```java
SparkConf conf = new SparkConf().setAppName("Application name")
JavaSparkContext sc = new JavaSparkContext(conf);
```

## RDDs creation

Creation from file (each line is a element of the RDD).

```java
JavaRDD<String> lines = sc.textFile(inputFile, numPartitions);
```

Creation from local collections

```java
JavaRDD<T> distList = sc.parallelize(inputList, numPartitions);
```

## Save RDDs

Store on a file in HDFS, invoked on the RDD we want to save `lines.saveAsTextFile(outputPath);`\
Store in a local Java variable of the Driver (pay attention to size) `List<String> contentOfLines = lines.collect();`

## RDDs operations

- **Transformations:**
   RDD content can not be changed, transformations return a new RDD.\
    Computed lazily.\
    Transformations are optimized via lineage graph (DAG).
- **Actions:**
   Return results to the Driver program or write result in the storage.

We pass functions to Transformations/Actions [`Function<T,R>` / `Function<T1,T2,R>` / `FlatMapFunction<T,R>`].\
Named Classes: to pass a function we create a class that implements a single function and then we use new Class() as arg to the Transformation/Action function.\
Anonymous Classes: to pass a function we pass `new Function<T,R>() {}` as arg to the Transformation/Action function.\
Lambda Functions: to pass a function we pass a lambda (es `x -> x.contains("error")` as arg to the Transformation/Action function.

## Basic RDD Transformations

Analyze the content of a single RDD and return a new RDD: `filter()`, `map()`, `flatMap()`, `distinct()`, `sample()`.\
Analyze the content of two RDDs and return a new RDD: `union()`, `intersection()`, `subtract()`, `cartesian()`.

- **Filter:**

   ```java
   JavaRDD<String> errorsRDD = inputRDD.filter(element -> element.contains("error"));
   JavaRDD<Integer> greaterRDD = inputRDD.filter(element -> { return (element > 2) });
   ```

- **Map:**

   ```java
   JavaRDD<Integer> lengthsRDD = inputRDD.map(element -> new Integer(element.length()));
   JavaRDD<Integer> squaresRDD = inputRDD.map(element -> new Integer(element ** element));
   ```

- **FlatMap:**
   Create a new RDD by applying f on each element x of the input RDD. f applied on x returns a list of values [y]).\

   ```java
   JavaRDD<String> listOfWordsRDD = inputRDD.flatMap(x -> Array.asList(x.split("")).iterator());
   ```

- **Distinct:**
   Create a new RDD containing the list of distinct elements of input RDD. Associated with shuffle (heavy operation).\

   ```java
   JavaRDD<String> distinctNamesRDD = inputRDD.distinct();
   ```

- **Sample:**
   Create a new RDD containing a random sample of the elements of input RDD.

   ```java
   JavaRDD<String> randomSentencesRDD = inputRDD.sample(false, 0.2); // without replacement, fraction 0.2 (20%)
   JavaRDD<String> randomSentencesRDD = inputRDD.sample(true, 0.3); // replacement, fraction 0.3 (30%)
   ```

- **Union:**
   (duplicates not removed).

   ```java
   JavaRDD<Integer> outputUnionRDD = inputRDD1.union(inputRDD2);
   ```

- **Intersection:**
   Intersection without duplicates. Associated with shuffle (heavy operation).

   ```java
   JavaRDD<Integer> outputIntersectionRDD = inputRDD1.intersection(inputRDD2);
   ```

- **Subtract:**
   Result contains the elements appearing only in the RDD on which the method is invoked. Associated with shuffle (heavy operation).

   ```java
   JavaRDD<Integer> outputIntersectionRDD = inputRDD1.subtract(inputRDD2);
   ```

- **Cartesian:**
   Result is a RDD of pairs containing all the combinations composed by one element per RDD.\

   ```java
   JavaPairRDD<Integer, Integer> outputCartesianRDD = inputRDD1.cartesian(inputRDD2);
   ```

## Basic RDD Actions

Can retrieve the content of an RDD or the result of a function applied on an RDD and store it in a local Java var or in an output folder/database.\
Returning objects -> `collect()`, `count()`, `countByValue()`, `take()`, `top()`, `takeSample()`, `reduce()`, `fold()`, `aggregate()`, `foreach()`

- **Collect:**
   List of objects containing the same objects of the RDD. USE ONLY IF YOU ARE SURE THAT THE LIST IS SMALL, OTHERWISE USE saveAsTextFile method.

   ```java
   List<Integer> retrievedValues = inputRDD.collect();
   ```

- **Count:**

   ```java
   long numLinesDoc = inputRDD.count();
   ```

- **CountByValue:**
   Map containing info about the number of times each element occurs in the RDD. USE ONLY IF YOU ARE SURE THAT THE LIST IS SMALL, OTHERWISE USE `saveAsTextFile` method.

   ```java
   java.util.Map<String, java.lang.Long> namesOccurrences = namesRDD.countByValue();
   ```

- **Take:**
   List of objects containing the first n elements of the RDD.

   ```java
   List<Integer> retrievedValues = inputRDD.take(2);
   ```

- **First:**
   Object containing the first element of the RDD.

   ```java
   Integer retrievedValue = inputRDD.first();
   ```

- **Top:**
   List of objects containing the top n largest elements of the RDD.

   ```java
   List<Integer> retrievedValues = inputRDD.top(2);
   ```

- **TakeOrdered:**
   List of objects containing the top n smallest elements of the RDD
- **TakeSample:**
   List of objects containing n random elements of the RDD.

   ```java
   List<Integer> randomValues = inputRDD.takeSample(false, 2); // in this case "with replacement = false"
   ```

- **Reduce:**
   Object obtained by combining the objects of the RDD using a user-provided function which must be associative and commutative (associative or the result will depend on how the RDD is partitioned).

   ```java
   Integer sum = inputRDD.reduce((element1, element2) -> element1 + element2);
   Integer sum = inputRDD.reduce((element1, element2) -> return (element1 > element2 ? element1 : element2));
   ```

- **Fold:**
   Object obtained by combining the objects of the RDD using a user-provided function which must be associative. Has a zero value
- **Aggregate:**
   Object obtained by combining the objects of the RDD and an initial zero value by using two user provided functions which must be associative. Result can be instance of a completely different class with respect to the starting RDD) STRANGE EXAMPLE WITH CLASS AND 2 FUNCTIONS

## Pair RDDs

Specific operations -> reduceByKey(), join() + the ones available for the standard RDDs.\
Allows grouping data by keys (similar to Map/Reduce).

- Built from regular RDDs with `mapToPair()` or `flatMapToPair()`.

   ```java
   JavaPairRDD<String, Integer> nameOneRDD = namesRDD.mapToPair(name -> new Tuple2<String, Integer>(name, 1));
   JavaPairRDD<String, Integer> wordOneRDD = linesRDD.flatMapToPair(line -> {
      List<Tuple2<String, Integer>> pairs = new ArrayList<>();
      String[] words = line.split(" ");
      for (String word : words) {
         pairs.add(new Tuple2<String, Integer>(word, 1));
      }
      return pairs.iterator();
   });
   ```

- Built from other pairs by transformations.
- From a Java collection with `parallelizePairs()` (Java Tuple2 stolen from scala with methods \_1() and \_2()).

   ```java
   JavaPairRDD<String, Integer> nameAgeRDD = sc.parallelizePairs(nameAge); // nameAge is ArrayList<Tuple2<String, Integer>>
   ```

## Transformations on Pair RDDs

- **ReduceByKey:**
   Associative and commutative function. Associated with shuffle (heavy operation).

   ```java
   JavaPairRDD<String, Integer> youngestPairRDD = nameAgeRDD.reduceByKey((age1, age2) -> age1 < age2 ? age1 : age2);
   ```

- **FoldByKey**:
   Zero value, associative function. Associated with shuffle (heavy operation).
- **CombineByKey:**
   Input and returned RDD can have different data type. Associated with shuffle (heavy operation) STRANGE EXAMPLE WITH CLASS AND 2 FUNCTIONS.
- **GroupByKey:**
   Reduce and aggregate/combine best performance for aggregation. Useful to apply a non associative function. Associated with shuffle (heavy operation).

   ```java
   JavaPairRDD<String, Iterable<Integer>> agesPerNamePairRDD = nameAgeRDD.groupByKey(); // nameAgeRDD is ArrayList<Tuple2<String, Integer>>
   ```

- **MapValues:**

   ```java
   JavaPairRDD<String, Integer> nameAgePlusOneRDD = nameAgeRDD.mapValues(age -> new Integer(age+1));
   ```

- **FlatMapValues:**
- **Keys:**
   List of keys.
- **Values:**
   List of values.
- **SortByKey:**
   Associated with shuffle (heavy operation).

   ```java
   JavaPairRDD<String, Integer> sortedNameAgeRDD = nameAgeRDD.sortByKey();
   ```

## Transformations on pairs of Pair RDDs

- **SubtractByKey:**
   Pair containing only the pairs associated with a key that is not appearing as key in the pairs of the other RDD. Associated with shuffle (heavy operation).

   ```java
   JavaPairRDD<String, Integer> selectedUsersPairRDD = profilesPairRDD.subtractByKey(bannedPairRDD);
   ```

- **Join:**
   Join two RDDs based on the value of the key of the pairs. Associated with shuffle (heavy operation).

   ```java
   JavaPairRDD<Integer, Tuple2<String, String>> joinPairRDD = questionsPairRDD.join(answersPairRDD);
   ```

- **CoGroup:**
   Associate each key with the list of values associated with the key on the input pair RDD and the list of values associated with the key on the other pair RDD. Associated with shuffle (heavy operation).

   ```java
   JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> cogroupPairRDD = moviesPairRDD.cogroup(directorsPairRDD);
   ```

## Actions on Pair RDDs

- **CountByKey:**
   Map containing the info about the number of elements associated with each key.

   ```java
   java.util.Map<String, java.lang.Object> movieNumRatings = movieRatingRDD.countByKey();
   ```

- **CollectAsMap:**
   Map containing the same pairs.

   ```java
   java.util.Map<String, String> retrievedPairs = usersRDD.collectAsMap();
   ```

- **Lookup:**
   Map containing the values of the pairs associated with the specified key.

   ```java
   java.util.List<Integer> movieRatings = movieRatingRDD.lookup("Forrest Gump");
   ```

## Double RDDs

JavaDoubleRDD is an RDD of doubles (different from `JavaRDD<Double>`).\
Has the actions -> `sum()`, `mean()`, `stdev()`, `variance()`, `max()`, `min()`

- **MapToDouble:**

   ```java
   JavaDoubleRDDlenghtsDoubleRDD = surnamesRDD.mapToDouble(surname -> (double)surname.length());
   ```

- **FlatMapToDouble:**

   ```java
   JavaDoubleRDD wordLenghtsDoubleRDD = sentencesRDD.flatMapToDouble(sentence -> {
      String[] words=sentence.split(" ");
      ArrayList<Double> lengths=new ArrayList<Double>();
      for (String word: words) {
         lengths.add(new Double(word.length()));
      }
      return lengths.iterator();
   });
   ```

## Persistence and Cache

Vantage if same RDD used multiple times.\
Can mark an RDD to be persisted by using the `persist()` method with different parameters from the class StorageLevel (es `StorageLevel.MEMORY_ONLY()`).\
Can cache an RDD by using cache() method (correspons to persist(`StorageLevel.MEMORY_ONLY()`)).\
They both return a new RDD.\
Spark automatically revomes cached/persisted RDDs but `unpersist()` method is available.

```java
JavaRDD<String> inputRDD = sc.textFile("./file.txt").cache();
```

## Accumulators

Shared variable that are "added" only through an associative operation (parallel support). Used to implement counters (as in MapReduce) or sums.\
ATTENTION: accumulators inside transformations (transformations are lazily evaluated).\
Put them in transformations/action that are executed only one time in the application.\
`foreach()` used frequently to update accumulators.\
Spark supports "`LongAccumulator longAccumulator()`" and "`DoubleAccumulator doubleAccumulator()`" with `add()` and `value()` methods.

```java
final LongAccumulator invalidEmails = sc.sc().longAccumulator();
invalidEmails.add(1);
System.out.println(invalidEmails.value());
```

We can define personalized accumulators by extending `org.apache.spark.util.AccumulatorV2<T,T>` and implementing the members

## Broadcast variables

Read-only medium/large shared variable, instantiated in the driver and sent to all worker nodes that use it in one or more Spark actions.\
Used to share large lookup-tables.

```java
HashMap<String, Integer> dictionary = new HashMap<String, Integer>();
for(Tuple2<String, Integer> pair : dictionaryRDD.collect()) {
   dictionary.put(pair._1(), pair._2());
}
final Broadcast <HashMap<String, Integer> dictionaryBroadcast = sc.broadcast(dictionary)
```
