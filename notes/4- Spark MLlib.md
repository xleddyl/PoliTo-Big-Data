# Spark MLlib

Machine learning and data mining algorithms (pre-processing, classification, clustering, itemset mining) (mllib and ml packages)

- [Data types](#data-types)
- [Main concepts](#main-concepts)
- [Classification algorithms](#classification-algorithms)
- [Categorical class labels](#categorical-class-labels)
- [Sparse labeled data](#sparse-labeled-data)
- [Textual data classification](#textual-data-classification)
- [Parameter tuning](#parameter-tuning)
- [Clustering algorithms](#clustering-algorithms)
- [Itemset and Association rule mining](#itemset-and-association-rule-mining)
- [Regression algorithms](#regression-algorithms)

## Data types

Local vector, Labeled point, Local matrix, Distributed matrix.\
DataFrames for ML contain objects based on thode basic data types

- **Local vectors:**
   Used to store vectors of double values. dense format [1.0 ,0.0 ,3.0] or sparse format (3, [0, 2], [1.0, 3.0]) (len, index non 0, values non zero)

   ```java
   Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
   Vector dv = Vectors.dense(1.0, 0.0, 3.0);
   ```

- **Labeled points:**
   Local vectors of doubles associated with a label, the label is a double. used by supervised algorithms to represent records/data points

   ```java
   LabeledPoint record1 = new LabeledPoint(1, Vectors.dense(1.0, 0.0, 3.0));
   LabeledPoint record2 = new LabeledPoint(0, Vectors.dense(2.0, 5.0, 3.0));
   ```

## Main concepts

Spark MLlib uses DataFrames as input data (input are structured data).\
All imputs must be represented by means of "tables" before applying the MLlib algorithms.\
Transformer: ML algorithm that transforms one DataFrame into another.\
Estimator: ML algorithm that is applied on a DataFrame to produce a Transformer (model).\
Pipeline: Chains multiple Transformers and Estimators together to specify a ML/DM workflow.\
Parameter: All transformes and Estimators share common APIs for specifying parameters

- **Pipeline approach:** prefered
   1. The set of Transformers and Estimators are instantiated
   2. A pipeline object is created and the sequence specified
   3. The pipeline is executed and a model is created
   4. The model is applied on new data

## Classification algorithms

Two main phases -> model generation and prediction.\
Only numerical attributes.\
All algorithms built on top of an input DataFrame containing at least two columns (label and features).

- **Logistic regression and structured data:**

   ```java
   SparkSession ss = SparkSession.builder().appName("logistic regression").getOrCreate();
   JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
   // training
      JavaRDD<String> trainingData = sc.textFile(inputFileTraining);
      JavaRDD<LabeledPoint> trainingRDD = trainingData.map(recors -> {
         String[] fields = record.split(",");
         double classLabel = Double.parseDouble(fields[0]);
         double[] attributesValue = new double[3];
         attributesValues[0] = Double.parseDouble(fields[1]);
         attributesValues[1] = Double.parseDouble(fields[2]);
         attributesValues[2] = Double.parseDouble(fields[3]);
         Vector attrValues = vectors.dense(attributesValues);
         return new LabeledPoint(classLabel, attrValues);
      });
      Dataset<Row> training = ss.createDataFrame(trainingRDD, LabeledPoint.class).cache();
      LogisticRegression lr = new Logisticregression();
      lr.setMaxIter(10);
      lr.setRegParam(0.01);
      Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{lr});
      PipelineModel model = pipeline.fit(training);
   // prediction
      JavaRDD<String> unlabeledData = sc.textFile(inputFileTest);
      JavaRDD<LabeledPoint> unlabeledRDD = unlabeledData.map(recors -> {
         String[] fields = record.split(",");
         double[] attributesValue = new double[3];
         attributesValues[0] = Double.parseDouble(fields[1]);
         attributesValues[1] = Double.parseDouble(fields[2]);
         attributesValues[2] = Double.parseDouble(fields[3]);
         Vector attrValues = vectors.dense(attributesValues);
         return new LabeledPoint((double)-1, attrValues);
      });
      Dataset<Row> test = ss.createDataFrame(unlabeledRDD, LabeledPoint.class);
      Dataset<Row> predictions = model.transform(test);
      Dataset<Row> predictionsDF = predictions.select("features", "prediction");
   // save result
      JavaRDD<Row> predictionsRDD = predictionsDF.javaRDD();
      predictionsRDD.saveAsTextFile(outputPath);
      sc.close();
   ```

- **Decision trees and structured data:**
   As before except for

   ```java
   DecisionTreeClassifier dc = new DecisionTreeClassifier();
   dc.setImpurity("gini");
   Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {dc});
   ```

## Categorical class labels

Frequently the class label is a categorical value (string).\
The Estimators StringIndexer and IndexToString support the transformation of categorical class label into numerical one.\
As before except we define `MyLabeledPoint` as the encoder.

```java
public class MyLabeledPoint implements Serializable {
   private String categoricalLabel;
   private Vector features;
   public MyLabeledPoint(String categoricalLabel, Vector features) {
      this.categoricalLabel = categoricalLabel;
      this.features = features;
   }
   public String getCategoricalLabel() {
      return categoricalLabel;
   }
   public Vector getFeatures() {
      return features;
   }
   public void setFeatures(Vector features) {
      this.features = features;
   }
}
```

And we define a `StringIndexer`

```java
StringIndexerModel labeledIndexer = new StringIndexer().setInputCol("categoricalLabel").setOutputCol("label").fit(training);
IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabeld(labelIndexer.labels());
Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {labelIndexer, dc, labelConverter});
```

## Sparse labeled data

MLlib supports reading training samples stored in the LIBSVM format.\
Textual format in which each line represents a labeled point by using sparse feature vectors.

```java
Dataset<Row> data = ss.read().format("libsvm").load("sample_libsvm_data.txt");
```

## Textual data classification

LR for textual documents

1. The textual part of the input data must be translated in a set of attributes in order to represent the data as a table
2. Conjunction words removed
3. TD-IDF measure to assign a different importance to the words based on their frequency

```java
Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
StopWordsRemoverremover = new StopWordsRemover().setInputCol("words").setOutputCol("filteredWords");
HashingTF hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("filteredWords").setOutputCol("rawFeatures");
IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {tokenizer, remover, hashingTF, idf, lr});
```

## Parameter tuning

Brute force approach.\
Cross-validation.\
Spark supports a brute-force grid-based approach.

## Clustering algorithms

In Spark they work only with numerical data.\
Input is DataFrame with a column called features.\
Clustering algorithm clusters the input records by considering only the content of features.

## Itemset and Association rule mining

FP-growth algorithm and rule mining algorithms

## Regression algorithms

AAA
