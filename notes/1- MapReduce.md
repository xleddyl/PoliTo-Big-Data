# Hadoop and MapReduce

Can parallelize large-scale batch computations on very large amount of data but with high latency.\
Batch layer: data stored in raw form and batch processing on data (result in batch views).\
Speed layer: analyzes data in real time, low latency at the expense of accuracy.\
Lambda: all data dispatched to both layers.

Typical Big Data Problem: Iterate over large number of records, extract something of interest from each record, aggregate intermediate results, generate final output.\
HDFS: data split in chunks (usually each chunk is replicated 3 times on different servers), one chunk contains part of the content of one single file, typically each chink is 64-128MB.\
Map: applied over each element of a input data set and emits a set of (key, value) pairs (transformation)\
Reduce: applied over each set of (key, value) pairs with the same key and emits a set of (key, value) pairs (aggregate).\
Shuffle and sort phase is always the same and it's already provided by the Hadoop system (performs the group by after the Map).\
Both input and output are lists of k-vue pairs.

Developers focus on the definition of the Map and Reduce functions (Driver, Mapper and Reducer classes).\

- Driver: main() method, configures the job, submits the job to Hadoop Cluster, runs on client machine.\
- Mapper: map() method, implements the map phase, runs on the cluster (intermediate results stored in the node, not the HDFS).\
- Reducer: reduce() method, implements the reduce phase, runs on the cluster (result stored in the HDFS).\

User-specified number of Reducers.

- [DataTypes](#data-types)
- [Combiner](#combiner)
- [Sharing parameters among Driver, Mappers and Reducers](#sharing-parameters-among-driver-mappers-and-reducers)
- [Counters](#counters)
- [Map only job](#map-only-job)
- [In-Mapper combiner](#in-mapper-combiner)
- [Summarization Patterns](#summarization-patterns)
- [Filtering Patterns](#filtering-patterns)
- [Data Organization Patterns](#data-organization-patterns)
- [Metapatterns](#metapatterns)
- [Join Patterns](#join-patterns)
- [Multiple Inputs](#multiple-inputs)
- [Multiple Outputs](#multiple-outputs)
- [Distributed cache](#distributed-cache)
- [MapReduce and Relational Algebra Operations](#mapreduce-and-relational-algebra-operations)

## Data Types

Text (String), IntWritable (Integer), LongWritable (Long), FloatWritable (Float).\
Writable and WritableComparable interfaces (all keys are WritableComparable) (all values are Writable).\
TextInputFormat: used to read the input data and logically transform the input HDFS file in a set of k-v pairs (files broken into lines). One k-v pair for each line, key offset of line in file, val content of line.\
KeyValueInputFormat: specify the separator for splitting lines (ended with \n). One k-v pair for each line, key before separator, val after separator.\
TextOutputFormat: `key\tvalue\n.`

```java
// Driver
// run method
   Configuration conf = this.getConf();
   Job job = Job.getInstance(conf);
   job.setJobName("aaa");
   FileInputFormat.addInputPath(job, inputPath);
   FileOytputFormat.setOutputPath(job, outputDir);
   job.setInputFormatClass(TextInputFormat.class);
   job.setOutputFormatClass(TextOutputFormat.class);
   job.setJarByClass(MapReduceAppDriver.class);
   job.setMapperClass(MyMapperClass.class);
   job.setMapOutputKeyClass(outputkeytype.class);
   job.setMapOutputValueClass(outputvaluetype.class);
   job.setReducerClass(MyReducerClass.class);
   job.setOutputKeyClass(outputkeytype.class);
   job.setOutputValueClass(outputvaluetype.class);
   job.setReduceTasts(numberOfReducers);
   int exitCode = job.waitForCompletion(true) == true ? 0 : 1;
   return exitCode
// main method
   int res = ToolRunner.run(new Configuration(), new MapReduceAppDriver(), args);
   System.exit(res);

// Mapper
// map method
   protected void map(MapperInputKeyType key, MapperInputValueType value, Context context) throws IOException InterruptedException {
      // process the input k-v pairs and call
      // context.write(new outputkey, new outputvalue);
   }

// Reducer
// reduce method
   protected voir reduce(ReducerInputKeyType key, Iterable<ReducerInputValueType> values, Context context) throws IOException InterruptedException {
      // process the input k-[list of v] pairs and call
      // context.write(new outputkey, new outputvalue);
   }
```

WordCount example:

```java
class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
   protected void map(LongWritable key, Text value, Context context) throws IOException InterruptedException {
      String[] words 0 value.toString().split("\\s+");
      for(String word : words) {
         String cleanedWord = word.toLowerCase()
         context.write(new Text(cleanedWord), new IntWritable(1)) // emit one pair for each word
      }
   }
}
class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
   protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException InterruptedException {
      int occ = 0;
      for(IntWritable value : values) {
         occ = occ + value.get() // count occurrences of each word
      }
      context.write(key, new IntWritable(occ)); // emit total occurrences of each word
   }
}
```

## Combiner

Some pre-aggregations could be performed to limit the amount of network data by using Combiners (mini-reducers).\
Combiner is locally called on the output of the mapper and aggregates it.\
Works only if the reduce function is commutative and associative.\
Execution of combiners not guaranteed, MapReduce job should not depend on the Combiner execution.\
Extends the Reducer class, reduce() method, runs on the cluster, process k-[list of v] pairs and emits k-v pairs.

```java
job.setCombinerClass();
class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
   protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException InterruptedException {
      for(IntWritable value : values) {
         occ = occ + value.get() // count occ of each word
      }
      context.write(key, new IntWritable(occ));
   }
}
```

Combiner and Reducer are basically the same, we can use `job.setCombinerClass(WordCountReducer.class);` (in 99% of applications this is true)

## Sharing parameters among Driver, Mappers and Reducers

Using the configuration object, personalized property-name pairs can be specified in the driver.\
Useful to share small constant properties available only during the execution of the program

- Driver: `conf.set("name", "value");`
- Mapper/Reducer: `context.getConfiguration().get("name");`

## Counters

User-defined counters defined by meand of Java enum, incremented in the Mappers and Reducers.\
Final value available at the end of the job.\

- Mapper/Reducer: `context.getCounter(countername).increment(value);`
- Driver: `getCounters(); / findCounter();` to retrieve the final values
- Dynamic counters: `incrCounter("group name", "counter name", value);` useful when the set of counters in unknown at design time

```java
// Driver
   public static enum COUNTERS {
      ERROR_COUNT,
      MISSING_FIELDS_RECORD_COUNT
   }
// Mapper or Reducer
   context.getCounter(COUNTERS.ERROR_COUNT).increment(1);
// Driver
   Counter errorCounter = job.getCounters().findCounter(COUNTERS.ERROR_COUNT);
```

## Map only Job

Reduce, shuffle and sort phases are not executed.\
Output directly stored in the HDFS.\
Set number of reducers to 0

## In-Mapper combiner

Mapper characterized by a setup and a cleanup method.\

- `setup()`: called once for each mapper (prior to the map()), used to set variables
- `cleanup()`: called once for each mapper (after the many calls to map()), used to emit k-v pairs based on the values of the in-mapper variables

Same things also for the Reducer.\
Improvement over standard Combiners.\
Pay attention to the amount of used main memory

## Summarization Patterns

- **Numerical Summarizations:**
   Group records/objects by a field and calculate a numerical aggregate per group
  - Mapper: output k-v where k is the fields used to define groups, v is the fields used to compute aggregates
  - Reducers: set of v for each group-by k and compute final statistics for each group
  - Combiners: speed up
  - Uses: word count, record count (per group), min/max/count (per group), average/median/std (per group)
- **Inverted Index Summarizations:**
   Build an index from the input data to support faster searches or data enrichment
  - Mappers: output k-v where k is set of fields to index (keyword), v is an unique identifier
  - Reducers: concatenate the set corresponding to each keyword
  - Combiners: no
  - Uses: web search engine
- **Counting with Counters:**
   Compute count summarizations of data sets
  - Mappers: process each input and increment a set of counters
  - Reducer: map only
  - Combiner: map only
  - Uses: count number of records, count a small number of unique instances, summarizations

## Filtering Patterns

- **Filtering:**
   Filter out input records that are not of interest
  - Mapper: k primary key of the record, v selected record
  - Reducer: map only
  - Combiner: map only
  - Uses: record filtering, tracking events, distributed grep, data cleaning
- **Top K:**
   Select a small set of top K records according to a ranking function
  - Mapper: each mapper initializes an in-mapper top k list (initialized in the setup) (cleanup emits the k-v made of null-topk)
  - Reducer: single instance, computes the final top k list by merging the local list emitted by the mappers
  - Combiners: no
  - Uses: outlier analysis, select interesting data (ranking)
- **Distinct:**
   Find unique set of values/records
  - Mapper: emit one k-v for each input record, k input, v null
  - Reducer: emit one k-v for each input k-[list of v] pair, k input key, v null
  - Uses: duplicate data removal, distinct value selection

## Data Organization Patterns

- **Binning:**
   Organize/move the input records into categories
  - Driver: set the list of "bins/output files" by means of MultipleOutputs
  - Mapper: for each input k-v pair, select the output bin/file associated with it and emit a k-v in that file
  - Reducer: map only
  - Combiner: map only
- **Shuffling:**
   Randomize the order of the data
  - Mapper: emit one k-v for each input, k i a random key, v is the input record
  - Reducer: emit one k-v pair for each value in [list of v] of the input k-[list of v] pair

## Metapatterns

Used to organize the workflow of a complex application executing many jobs

- **Job Chaining:**
   Execute a sequence of jobs (synchronizing them)
  - Driver: contains the workflow of the app and executes the jobs in the proper order
  - Mapper, Reducer, Combiner: implemented by a MapReduce Job

## Join Patterns

Used to implement the join operators of the relational algebra (natural join)

- **Reduce side natural join:**
   Join the content of two relations
  - 2 Mappers: one mapper for each table, emit one k-v for each input, k is common attribute, v is concat of name of the table and the content of the record
  - Reducer: iterate over the values assocated with each key and compute the local natural join for the current key
- **Map side natural join:**
   Join the content of two relations
  - Mapper: processes the content of the large table, cache used to provide a copy of the small table to all the mappers, each mapper persorms the local
      natural join between the current record it is processing and the records of the small table
- **Other join:**
   Same patterns as before

## Multiple Inputs

Data read from two or more datasets.\
One mapper for each different dataset must be specified, output of different mappers must be consistent.\
Inside the Driver use `addInputPath` of the `MultipleInputs` class multiple times.

```java
MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper1.class);
```

## Multiple Outputs

Store the result of the application in different files inside the same ouptut directory
`MultipleOutputs` class used to specify prefixes of the output files.\
Inside the Driver use `MultipleOutputs.addNamedOutput` to specify the prefixes of output files.

```java
MultipleOutputs.addNamedOutput(job, "hightemp", TextOutputFormat.class, Text.class, NullWritable.class);
```

- If map only:
   1. define a private `MultipleOutput` var in the mapper private `MultipleOutputs<Text, NullWritable> mos = null;`
   2. inside the `setup()` add: `mos = new MultipleOutputs<Text, NullWritable>(context);`
   3. use the write method to write k-v pairs in the file of interest `mos.write("hightemp", key, value);`
   4. close the `MultipleOutputs` object with `mos.close();`

## Distributed cache

Files accessible by all nodes of the cluster.\
Inside the Driver `job.addCacheFile(new Path("hdfspath/filename").toUri());`.\
The shared file is read in the `setup()` method of the Mapper/Reducer.\
Inside the Mapper/Reducer `URI[] urisCachedFiles = context.getCacheFiles();`

## MapReduce and Relational Algebra Operations

- **Selection:**
   Applies condition C to each record of the table R
  - Mapper: analyzes one record at a time of its split, if record satisfies C then it emits a k-v with k=record v=null
  - Reducer: map only
  - Combiner: map only
- **Projection:**
   For each record of table R keeps only the attributes in S
  - Mapper: for each record r in R, selects the values of the attributes in S and construct a new record. emit k-v with k=newrecord v=null
  - Reducer: emits one k-v pair for each input k-[list of v] pair
  - Combiner: no
- **Union:**
   R an S have the same schema, produces a relation with the same schema of R and S, keep records appearing in R or in S, no duplicates
  - Mapper: for each input emit one k-v with k=record v=null
  - Reducers: emit one k-v for each input k-[list of v] with key=record, v=null
  - Combiner: no
- **Intersection:**
   R an S have the same schema, produces a relation with the same schema of R and S, keep records appearing in R and in S
  - Mapper: for each record emit a k-v with k=record v=tablename
  - Reducer: emit one k-v with k=record v=null for each k-[list of v] with [list of v] containing two values
  - Combiner: no
- **Difference:**
   R an S have the same schema, produces a relation with the same schema of R and S, keep records appearing in R and not in S
  - Mapper: for each input emit k-v with k=record v=tablename (one mapper for each relation)
  - Reducers: emit one k-v with k=record v=null for each k-[list of v] with [list of v] containing only R
  - Combiner: no
- **Join:**
   Implemented by using Join Pattern
- **Aggregations and GroupBy:**
   Implemented by using Summarization Pattern
