# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 04 : Hyper-parameter Tuning
# MAGIC
# MAGIC In the practical machine learning works, it’s very hard to find best parameters - such as learning rare (in neural networks), iterations or epoch, [regression family](https://tsmatz.wordpress.com/2017/08/30/glm-regression-logistic-poisson-gaussian-gamma-tutorial-with-r/), kernel functions (in svm etc), [regularization parameters](https://tsmatz.wordpress.com/2017/09/13/overfitting-for-regression-and-deep-learning/), so on and so forth. In Spark machine learning, you can quickly find best parameters by scaling Spark massive workers.
# MAGIC
# MAGIC In this lab, we change the source code of Exercise 03 and find the best values of parameters - "learningRate" and "numLeaves" - for LightGBM classifier in Exercise 03 by grid search. (Here we explore only classification's parameters, but you can also tune transformation's parameters.)
# MAGIC
# MAGIC Before starting,
# MAGIC
# MAGIC - Download [flight_weather.csv](https://1drv.ms/u/s!AuopXnMb-AqcgbZD7jEX6OTb4j8CTQ?e=KkeDdT) into your blob container. (See "Exercise 01 : Storage Settings".)
# MAGIC - Install MMLSpark to use LightGBM into your cluster. (See "Exercise 03 : Spark Machine Learning Pipeline".)
# MAGIC
# MAGIC > Note : You can also use ```CrossValidator()``` instead of using ```TrainValidationSplit()```, but please be care for training overheads when using ```CrossValidator()```.    
# MAGIC The word "Cross Validation" means : For example, by setting ```numFolds=5``` in ```CrossValidator()```, 4/5 is used for training and 1/5 is for testing, and moreover each pairs are replaced and averaged. As a result, 5 pairs of dataset are used and the training occurs (3 params x 3 params) x 5 pairs = 45 times. (See "[ML Tuning: model selection and hyperparameter tuning](https://spark.apache.org/docs/latest/ml-tuning.html)" in official Spark document.)
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# Read dataset
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
df = (sqlContext.read.format("csv").
  option("header", "true").
  option("nullValue", "NA").
  option("inferSchema", True).
  load("abfss://container01@demostore01.dfs.core.windows.net/flight_weather.csv"))

# COMMAND ----------

# ARR_DEL15 = 1 if it's canceled.
from pyspark.sql.functions import when
df = df.withColumn("ARR_DEL15", when(df["CANCELLED"] == 1, 1).otherwise(df["ARR_DEL15"]))

# COMMAND ----------

# Remove flights if it's diverted.
df = df.filter(df["DIVERTED"] == 0)

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

# Drop rows with null values
df = df.dropna()

# COMMAND ----------

# Convert categorical values to indexer (0, 1, ...)
from pyspark.ml.feature import StringIndexer
uniqueCarrierIndexer = StringIndexer(inputCol="UNIQUE_CARRIER", outputCol="Indexed_UNIQUE_CARRIER").fit(df)
originIndexer = StringIndexer(inputCol="ORIGIN", outputCol="Indexed_ORIGIN").fit(df)
destIndexer = StringIndexer(inputCol="DEST", outputCol="Indexed_DEST").fit(df)
arrDel15Indexer = StringIndexer(inputCol="ARR_DEL15", outputCol="Indexed_ARR_DEL15").fit(df)

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

# Define classifier
from mmlspark.lightgbm import LightGBMClassifier
classifier = LightGBMClassifier(featuresCol="features", labelCol="ARR_DEL15", numIterations=100)

# COMMAND ----------

# Create pipeline
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[uniqueCarrierIndexer, originIndexer, destIndexer, arrDel15Indexer, assembler, classifier])

# COMMAND ----------

# MAGIC %md
# MAGIC Note : The following execution will take a long time, because of a serial evaluation by default.    
# MAGIC Use ```setParallelism()``` to improve performance.

# COMMAND ----------

# Run pipeline with ParamGridBuilder
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# 3 x 3 = 9 times training occurs
paramGrid = ParamGridBuilder() \
 .addGrid(classifier.learningRate, [0.1, 0.3, 0.5]) \
 .addGrid(classifier.numLeaves, [100, 150, 200]) \
 .build()
# Set appropriate parallelism by setParallelism() in production
# (It takes a long time)
tvs = TrainValidationSplit(
  estimator=pipeline,
  estimatorParamMaps=paramGrid,
  evaluator=MulticlassClassificationEvaluator(labelCol="ARR_DEL15", predictionCol="prediction"),
  trainRatio=0.8)  # data is separated by 80% and 20%, in which the former is used for training and the latter for evaluation
model = tvs.fit(df)

# COMMAND ----------

# View all results (accuracy) by each params
list(zip(model.validationMetrics, model.getEstimatorParamMaps()))

# COMMAND ----------

# Predict using best model
df10 = df.limit(10)
model.bestModel.transform(df10)\
  .select("ARR_DEL15", "prediction")\
  .show()

# COMMAND ----------

