# Spark SQL

Provides an abstraction called Datasat that can act as a distributed SQL.\
Datasets faster than RDD-based programs.\
Structured data.\
Dataset: distributed collection of structured data.\
DataFrame: particular Dataset organized into named columns (Dataset of Row objects).

```java
SparkSession ss = SparkSession.builder().appName("aaa").getOrCreate();
ss.stop()
```

- [DataFrames](#dataframes)
- [Datasets](#datasets)
- [Datasets operations](#datasets-operations)
- [Aggregates functions](#aggregates-functions)
- [Datasets, DataFrames and the SQL language](#datasets-dataframes-and-the-sql-language)
- [Save Datasets and DataFrames](#save-datasets-and-dataframes)
- [User defined functions UDFs](#user-defined-functions-udfs)

## DataFrames

Distributed collection of data organized into named columns (relational table) (`Dataset<Row>`)

- **From CSV:**

   ```java
   DataFrameReader dfr = ss.read().format("csv").option("header", true).option("inferSchema", true);
   Dataset<Row> df = dfr.load("persons.csv");
   ```

- **From JSON:**
   JSON Lines text format (newline-delimited JSON)

   ```java
   DataFramereader dfr = ss.read().format("json");
   //multiple files -> DataFramereader dfr = ss.read().format("json").option("multiline", true);
   Dataset<Row> df = dfr.load("persons.json");
   //multiple files -> Dataset<Row> df = dfr.load("folder_JSONFiles/");
   ```

- **From DataFrame to RDD:**
   Important methods of the Row class: `fieldIndex(string)`, `getAs(string)`, `getString(int)`, `getDouble(int)`

   ```java
   JavaRDD<Row> rddPersons = df.javaRDD();
   JavaRDD<String> rddNames = rddPersons.map(inRow -> (String)inRow.getAs("name"));
   // or .map(inRow-> inRow getString(inRow.fieldIndex("name")));
   ```

## Datasets

More general than DataFrames, Datasets are collections of objects.\
More efficient than RDDSs, special encoders to serialize the objects for processing or transmission over the network.\
Object stored must be JavaBean-compliant (class that implements Serializable and has private attributes and public getters/setters).

- **From a collection:**

   ```java
   public class Person implements Serializable{
      // private attributes and public getters/setters
   }
   ArrayList<Person> persons = new ArrayList<Person>(); // add persons
   Encoder<Person> personEncoder = Encoders.bean(Person.class); // default encoders ad Encoders.INT(), Encoders.STRING()
   Dataset<Person> personDS = ss.createDataset(person, personEncoder);
   ```

- **From DataFrames:**

   ```java
   DataFrameReader dfr = ss.read().format("csv").option("header", true).option("inferSchema", true);
   Dataset<Row> df = dfr.load("persons.csv");
   Encoder<Person> personEncoder = Encoders.bean(Person.class);
   Dataset<Person> ds = df.as(personEncoder);
   ```

- **From CSV or JSON:**
   Define a dataFrame based on the input files, convert it into a Dataset by using the .as() method and an Encoder object (as before)
- **From RDDs:**
   Pay attention that the first parameter is a scala RDD and not a JavaRDD

## Datasets operations

`show()`, `printSchema()`, `count()`, `distinct()`, `select()`, `filter()`, `map()`, `flatMap()`, ...

- **Show:**
   Prints the first n Rows or all the Rows of the Dataset.

   ```java
   ds.show(2);
   ds.show();
   ```

- **PrintSchema:**
   Prints the schema of the Dataset.

   ```java
   ds.printSchema();
   ```

- **Count:**
   Number of rows in the input Dataset (Long).

   ```java
   System.out.println("The file contains " + ds.count() + " persons");
   ```

- **Distinct:**
   Returns a new Dataset that contains only the unique rows of the input Dataset.

   ```java
   Dataset<String> distinctNames = ds.distinct();
   ```

- **Select:**
   Returns a new DataFrame that contains only the specified columns of the input Dataset (errors at runTime).

   ```java
   Dataset<Row> dfNamesAges = ds.select("name", "age");
   ```

- **SelectExpr:**
   Returns a new DataFrame containing a set of columns computed by combining the original colums (errors at runTime).

   ```java
   Dataset<RowZ df = ds.selectExpr("name", "age", "gender", "age+1 as newAge");
   ```

- **Map:**
   Encoder used to encode returned objects (errors at compileTime).

   ```java
   Dataset<PersonNewAge> ds2 = ds.map(p -> {
      PersonNewAge newPersonNA = new PersonNewAge();
      newPersonNA.setName(p.getName());
      newPersonNA.setAge(p.getAge());
      newPersonNA.setGender(p.getGender());
      newPersonNA.setNewAge(p.getAge()+1);
      return newPersonNA;
   },
   Encoders.bean(PersonNewAge.class));
   ```

- **Filter:**
   (errors at runTime).

   ```java
   Dataset<Person> dsSelected = ds.filter("age >= 20 and age <= 31");
   ```

- **Filter with lambdas:**
   (errors at compileTime).

   ```java
   Dataset<Person> dsSelected = ds.filter(p -> { return (p.getAge() >= 20 && p.getAge() <= 31) });
   ```

- **Where:**
   Alias of the filter with expression
- **Join:**
   Return a DataFrame which is the join of two Datasets (errors at runTime).

   ```java
   Dataset<Row> dfPersonLikes = dsPersons.join(dsUidSports, dsPersons.col("uid").equalTo(dsUidSports.col("uid"), "inner"));
   // different join versions, leftanti for subtract
   ```

## Aggregates functions

Compute aggregates over the set of values of columns -> `avg(column)`, `count(column)`, `sum(column)`, `abs(column)`.\
`agg(aggregate functions)` used to specify which aggregate functions we want to apply (errors at runTime).

```java
Dataset<Row> averageAge = ds.agg(avg("age"), count("*"));
```

- **GroupBy:**
   Combined with a set of aggregate methods can be used to split the input data in groups and compute aggregate functions over each group (errors at runTime).

   ```java
   RelationalGroupedDataset rgd = ds.groupBy("name");
   Dataset<Row> nameAverageAge = rgd.avg("age");
   Dataset<Row> nameAverageAge = rgd.agg(avg("age"), count("name"));
   ```

- **Sort:**
   Returns a new Dataset in which the content is sorted by col1,...,coln (errors at runTime).

   ```java
   Dataset<Person> sortedAgeName = ds.sort(new Column("age").desc(), new Column("name"));
   // .desc() used to obtain descending order
   ```

## Datasets, DataFrames and the SQL language

Sparks allows querying the content of a Dataset also by using the SQL language, a table name must be assigned to each Dataset.

```java
ds.createOrReplaceTempView("people");
Dataset<Row> selectedPersons = ss.sql("SELECT \* FROM people WHERE age >= 20 and age <= 31");
```

## Save Datasets and DataFrames

- **Convert to RDDs and use saveAsTextFile():**

   ```java
   ds.JavaRDD().saveAsTextFile(outputPath);
   ```

- **DataFrameWriter:**

   ```java
   ds.write().format("csv").option("header", true).save(outputPath);
   ```

## User defined functions UDFs

Created by invoking `udf().register(String name, UDF function, DataType datatype)` on `JavaSparkSession`

```java
ss.udf().register("length", (String name) -> name.length(), DataTypes.IntegerType);
Dataset<Row> result = ss.sql("SELECT length(name) FROM profiles");
```
