# Spark Streaming

Framework for large scale stream processing, batch like API, hundreds of nodes.\
Process large stream of live data and provide results in near-real-time.

- [Discretized Stream Processing](#discretized-stream-processing)
- [Transformations](#transformations)
- [Actions](#actions)
- [Start and run](#start-and-run)
- [Example word count](#example-word-count)
- [Window operations](#window-operations)
- [Checkpoints](#checkpoints)
- [Stateful Transformations](#stateful-transformations)
- [Advanced Transformations](#advanced-transformations)

## Discretized Stream Processing

Computation as a series of very small deterministic batch jobs, each input stream split in portions and processed one portion (batch) at a time

1. Splits the live stream into batches of X seconds
2. Treats each batch of data as RDDs and processes them using RDD operations
3. Finally, the processed results of the RDD operations are returned in batches

DStream: sequence of RDDs representing a discretized version of the input stream of data.\
PairDStream: sequence of PairRDDs representing a stream of pairs.\
Transformations: modify data from one DStream to another (standard RDD operations).\
Actions: (Output Operations) send data to external entity.

1. Define SparkStreamingContext
2. Specify input stream and a DStream based on it
3. Specify the operations to execute for each batch of data
4. Invoke start method
5. Wait until the application is killed or the timeout specified int the app expires

```java
JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
// Content emitted by a TCP socket in localhost:9999
JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
```

Content of a HDFS folder: `JavaDStream<String> lines = jssc.textFileStream(inputFolder);`.\
Also on stream generated from applications

## Transformations

- **map(func)**
   Returns a new DStream by passing each element of the source DStream through a function func
- **flatMap(func):**
   Returns a new DStream where each input item can be mapped to one or more output items
- **filter(func):**
   Returns a new DStream selecting only the records on which func returns true
- **reduce(func):**
   Returns a new DStream of single-element RDDs by aggregating elements
- **reduceByKey(func):**
   When called on PairDStream returns a PairDStream where the values of each key are aggregated
- **countByValue():**
   When called on a DStream of elements of type K returns a new PairDStream where the key is its frequency
- **count():**
   Returns a new DStream of single-element RDDs by counting the number of elements in each batch
- **union(otherStream):**
   Returns a new DStream that contains the union
- **join(otherStream):**
   When called on two PairDStream returns a new PairDStream with all pairs of elements for each key
- **cogroup(otherStream):**
   When called on two PairDStream returns a new DStream of tuples

## Actions

- **print()**:
   Prints the first 10 elements of every batch of data
- **saveAsTextFile(prefix, [suffix]):**
   Saves the content of the DStream on which is invoked as text files `.dstream().saveAsTextFile()`

## Start and run

`start()` method used to start the application on th einput stream.\
`awaitTerminationOrTimeout(long milliseconds)` used to specify how long the app will run, no arg means run forever

## Example word count

```java
SparkConf conf = new SparkConf().setAppName("aaa");
JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
JavaDStream<String> words = lines
   .flatMap(line -> Array.asList(line.split("\\s+")).iterator());
JavaPairDStream<String, Integer> wordsOnes = words
   .mapToPair(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1));
JavaPairDStream<String, Inreger> wordsCounts = words
   .reduceByKey((i1, i2) -> i1 + i2);
wordsCounts.print();
wordsCounts.dstream().saveAsTextFiles(outputPathPrefix, "");
jssc.start();
jssc.awaitterminationOrTimeout(120000);
jssc.close();
```

## Window operations

Apply transformations over a sliding window of data.\
Each window contains a set of batches of the input stream windows can be overlapped.\
Parameters: window length, sliding interval (multiples of the batch interval of the source DStream)

- **window(windowLength, slideInterval):**
   Returns a new DStream which is computed based on windowed batches
- **countByWindow(windowLength, slideInterval):**
   Returns a new single-element stream containing the number of elements of each window
- **reduceByKeyAndWindow(func, windowLength, slideInterval):**
   When called on a PairDStream returns a PairDStream where the values for each key are aggregated over batches in a sliding windows

## Checkpoints

Operations that store the data and metadata needed to restart the computation if failures happen.\
Useful also for some window and stateful transformations.

```java
checkpoint(String folder) // method used to enable checkpoints
jssc.checkpoint("checkpointfolder"); // before defining the input stream
```

## Stateful Transformations

`UpdateStateByKey` allows mantaining a state (value continuously updated every time a batch is analyzed).\
Define the state and the state update function.\
Spark will apply the state update function for all existing keys.\
By using `UpdateStateByKey` the word count app can continuously update the number of occurences of each word.\

```java
JavaPairDStream<String, Integer> totalWordsCounts = wordsCounts.updateStateByKey((newValues, state) -> {
   Integer newSum = state.or(0);
   for(Integer value : newValues) {
      newSum += value;
   }
   return Optional.of(newSum);
});
```

## Advanced Transformations

- **transform(func):**
   Returns a new DStream by applying an RDD to RDD function to every RDD of the source DStream
- **transformToPair(func):**
   Returns a new PairDStream by applying a PairRDD to PairRDD function to every PairRDD of the source PairDStream
