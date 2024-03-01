# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 03 : Spark Machine Learning Pipeline
# MAGIC
# MAGIC In previous exercise, we saw the primitive training example compared with usual Python and Pandas dataframe. Here we see more practical machine learning example using **Spark ML pipeline**.    
# MAGIC Now we create the model to predict the flight delay over 15 minutes (ARR_DEL15) using other attributes, such as, airport code, career, and various weather conditions.
# MAGIC
# MAGIC Before starting,
# MAGIC
# MAGIC - You must put [flight_weather.csv](https://1drv.ms/u/s!AuopXnMb-AqcgbZD7jEX6OTb4j8CTQ?e=KkeDdT) in your blob container. (See "Exercise 01 : Storage Settings")
# MAGIC - You must install **MMLSpark** library (Microsoft Machine Learning Library for Apache Spark) to use LightGBM as follows.
# MAGIC     1. Select "Create" - "Library" menu on "Shared" in Workspace    
# MAGIC     ![Start to create library](https://tsmatz.github.io/images/github/databricks/20200423_create_library.jpg)
# MAGIC     2. In a "Create Library" wizard, insert mmlspark library as follows.<br>
# MAGIC     For Runtime version 6.x (Spark 2.x), set ```com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1``` in maven repository ```https://mmlspark.azureedge.net/maven```, and push "Create" button to proceed.<br>
# MAGIC     For Runtime version 7.x (Spark 3.x) or above, set ```com.microsoft.ml.spark:mmlspark_2.12:1.0.0-rc3-24-495af3e4-SNAPSHOT``` instead.<br>
# MAGIC     ![Set MMLSpark library in maven repository](https://tsmatz.github.io/images/github/databricks/20200423_install_mmlspark.jpg)
# MAGIC     3. In next dialog, select your cluster and push "Install" button    
# MAGIC     ![Install library in your cluster](https://tsmatz.github.io/images/github/databricks/20200423_install_oncluster.jpg)
# MAGIC     4. Restart your cluster
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# Read dataset from Azure Blob
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
df = (sqlContext.read.format("csv").
  option("header", "true").
  option("nullValue", "NA").
  option("inferSchema", True).
  load("abfss://container01@demostore01.dfs.core.windows.net/flight_weather.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC See original data

# COMMAND ----------

# See data
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Mark as "delayed over 15 minutes" if it's canceled.

# COMMAND ----------

# ARR_DEL15 = 1 if it's canceled.
from pyspark.sql.functions import when
df = df.withColumn("ARR_DEL15", when(df["CANCELLED"] == 1, 1).otherwise(df["ARR_DEL15"]))

# COMMAND ----------

# MAGIC %md
# MAGIC Remove flights if it's diverted.

# COMMAND ----------

# Remove flights if it's diverted.
df = df.filter(df["DIVERTED"] == 0)

# COMMAND ----------

# MAGIC %md
# MAGIC Narrow to required columns.

# COMMAND ----------

# Select required columns
df = df.select(
  "ARR_DEL15",
  "MONTH",
  "DAY_OF_WEEK",
  "UNIQUE_CARRIER",
  "ORIGIN",
  "DEST",
  "CRS_DEP_TIME",
  "CRS_ARR_TIME",
  "RelativeHumidityOrigin",
  "AltimeterOrigin",
  "DryBulbCelsiusOrigin",
  "WindSpeedOrigin",
  "VisibilityOrigin",
  "DewPointCelsiusOrigin",
  "RelativeHumidityDest",
  "AltimeterDest",
  "DryBulbCelsiusDest",
  "WindSpeedDest",
  "VisibilityDest",
  "DewPointCelsiusDest")

# COMMAND ----------

# MAGIC %md
# MAGIC Drop rows with null value for all columns

# COMMAND ----------

# Drop rows with null value
df = df.dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC Split data into training data and evaluation data (ratio is 80% : 20%)

# COMMAND ----------

# Split data into train data and test data
(traindf, testdf) = df.randomSplit([0.8, 0.2])

# COMMAND ----------

# MAGIC %md
# MAGIC Convert categorical values to index values (0, 1, ...) for carrier code (UNIQUE_CARRIER), airport code (ORIGIN, DEST), flag for delay over 15 minutes (ARR_DEL15).

# COMMAND ----------

# Convert categorical values to index values (0, 1, ...)
from pyspark.ml.feature import StringIndexer
uniqueCarrierIndexer = StringIndexer(inputCol="UNIQUE_CARRIER", outputCol="Indexed_UNIQUE_CARRIER").fit(df)
originIndexer = StringIndexer(inputCol="ORIGIN", outputCol="Indexed_ORIGIN").fit(df)
destIndexer = StringIndexer(inputCol="DEST", outputCol="Indexed_DEST").fit(df)
arrDel15Indexer = StringIndexer(inputCol="ARR_DEL15", outputCol="Indexed_ARR_DEL15").fit(df)

# COMMAND ----------

# MAGIC %md
# MAGIC In Spark machine learning, the feature columns must be wrapped as vector value.    
# MAGIC We create new vector column named "features".

# COMMAND ----------

# Assemble feature columns
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(
  inputCols = [
    "MONTH",
    "DAY_OF_WEEK",
    "Indexed_UNIQUE_CARRIER",
    "Indexed_ORIGIN",
    "Indexed_DEST",
    "CRS_DEP_TIME",
    "CRS_ARR_TIME",
    "RelativeHumidityOrigin",
    "AltimeterOrigin",
    "DryBulbCelsiusOrigin",
    "WindSpeedOrigin",
    "VisibilityOrigin",
    "DewPointCelsiusOrigin",
    "RelativeHumidityDest",
    "AltimeterDest",
    "DryBulbCelsiusDest",
    "WindSpeedDest",
    "VisibilityDest",
    "DewPointCelsiusDest"],
  outputCol = "features")

# COMMAND ----------

# MAGIC %md
# MAGIC Generate classifier. Here we use Light GBM classifier in MMLSpark.

# COMMAND ----------

# Generate classifier
from mmlspark.lightgbm import LightGBMClassifier
classifier = LightGBMClassifier(
  featuresCol="features",
  labelCol="ARR_DEL15",
  learningRate=0.3,
  numIterations=150,
  numLeaves=100)

# COMMAND ----------

# MAGIC %md
# MAGIC Generate SparkML pipeline and run training !    
# MAGIC Trained model (with coefficients) and pipeline are stored in "model".

# COMMAND ----------

# Create pipeline and Train
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[uniqueCarrierIndexer, originIndexer, destIndexer, arrDel15Indexer, assembler, classifier])
model = pipeline.fit(traindf)

# COMMAND ----------

# MAGIC %md
# MAGIC Predict with eveluation data

# COMMAND ----------

# Predict with eveluation data
pred = model.transform(testdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Show eveluation result. (I'm sorry, but it might not be good result in this example ...)

# COMMAND ----------

# Evaluate results
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="ARR_DEL15", predictionCol="prediction")
accuracy = evaluator.evaluate(pred)
print("Accuracy = %g" % accuracy)

# COMMAND ----------

# MAGIC %md
# MAGIC Save (Export) pipeline model with trained coefficients.    
# MAGIC Saved pipeline mode can be loaded on ACI or AKS using Azure Machine Learning service for inference serving.
# MAGIC
# MAGIC Before running, **you must run Exercise 01 and create mounted point (/mnt/testblob)**.

# COMMAND ----------

# Save pipeline
model.write().overwrite().save("/mnt/testblob/flight_model")

# COMMAND ----------

