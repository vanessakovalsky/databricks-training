# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 05 : MLeap
# MAGIC
# MAGIC Essentially MLeap has no dependency on Spark and it has own core, runtime, and also own dataframe (called "LeapFrame") including own transforms and learners corresponding to SparkML objects.
# MAGIC
# MAGIC You can train ML pipeline with Spark, then export generated Spark pipeline (not only model, but **entire pipeline**) into MLeap pipeline. After that, you can also import serialized pipeline into non-Spark runtime using MLeap framework. (See [here](https://github.com/combust/mleap#user-content-load-and-transform-using-mleap). Unlike MLFlow model format, MLeap doesn't have python binding and then use only scala and java.)
# MAGIC
# MAGIC In this hands-on, we simply export generated pipeline in Exercise 03 and import again into same Spark cluster (here) using MLeap.
# MAGIC
# MAGIC Before starting,
# MAGIC
# MAGIC - Create **Databricks Runtime ML** (5.0 or above) for cluster and attach in this notebook. (ML runtime includes MLeap libraries, instead of installing MLeap by yourself.)
# MAGIC - Make sure to configure blob access key on your Runtime ML cluster. (See "Exercise 01 : Storage Settings".)
# MAGIC
# MAGIC > Note : As you can see in Exercise 10, you can save Spark ML Pipeline models using MLFlow model format and load them as generic Python functions.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC Run Exercise 03 as follows.   
# MAGIC Here we've changed classifier to built-in ```DecisionTreeClassifier```, since ```LightGBMClassifier``` in mmlspark is not supported in MLeap. (Eventually, the model accuracy will become lower than Exercise 03 or 04.)
# MAGIC
# MAGIC The generated model (including entire pipeline) is stored in the variable "**model**" and predicted (transformed) dataframe is stored in "**pred**".

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
df = (sqlContext.read.format("csv").
  option("header", "true").
  option("nullValue", "NA").
  option("inferSchema", True).
  load("abfss://container01@demostore01.dfs.core.windows.net/flight_weather.csv"))

from pyspark.sql.functions import when
df = df.withColumn("ARR_DEL15", when(df["CANCELLED"] == 1, 1).otherwise(df["ARR_DEL15"]))

df = df.filter(df["DIVERTED"] == 0)

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

df = df.dropna()

(traindf, testdf) = df.randomSplit([0.8, 0.2])

from pyspark.ml.feature import StringIndexer
uniqueCarrierIndexer = StringIndexer(inputCol="UNIQUE_CARRIER", outputCol="Indexed_UNIQUE_CARRIER").fit(df)
originIndexer = StringIndexer(inputCol="ORIGIN", outputCol="Indexed_ORIGIN").fit(df)
destIndexer = StringIndexer(inputCol="DEST", outputCol="Indexed_DEST").fit(df)
arrDel15Indexer = StringIndexer(inputCol="ARR_DEL15", outputCol="Indexed_ARR_DEL15").fit(df)

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

# Note : Here we use DecisionTreeClassifier instead of LightGBMClassifier
from pyspark.ml.classification import DecisionTreeClassifier
classifier = DecisionTreeClassifier(
  featuresCol="features",
  labelCol="ARR_DEL15",
  maxDepth=15,
  maxBins=500)

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[uniqueCarrierIndexer, originIndexer, destIndexer, arrDel15Indexer, assembler, classifier])
model = pipeline.fit(traindf)

pred = model.transform(testdf)

# COMMAND ----------

# MAGIC %md
# MAGIC Create model folder on Spark driver. (Don't use dbfs !)

# COMMAND ----------

# MAGIC %sh 
# MAGIC rm -rf /tmp/models
# MAGIC mkdir /tmp/models

# COMMAND ----------

# MAGIC %md
# MAGIC Save entire pipeline in the previous folder.

# COMMAND ----------

import mleap.pyspark
from mleap.pyspark.spark_support import SimpleSparkSerializer
model.serializeToBundle("jar:file:/tmp/models/flight-delay-classify.zip", pred.limit(0))

# COMMAND ----------

# MAGIC %md
# MAGIC As I described earlier, you can load your pipeline on single node using MLeap runtime.    
# MAGIC But here we import pipeline on Databricks (Spark cluster) again.

# COMMAND ----------

from pyspark.ml import PipelineModel
loadedPipeline = PipelineModel.deserializeFromBundle("jar:file:/tmp/models/flight-delay-classify.zip")

# COMMAND ----------

# MAGIC %md
# MAGIC Predict using loaded pipeline.

# COMMAND ----------

# Predict
pred = loadedPipeline.transform(testdf)

# Compare results
comparelist = pred.select("ARR_DEL15", "prediction")
comparelist.cache()
display(comparelist)

# COMMAND ----------

