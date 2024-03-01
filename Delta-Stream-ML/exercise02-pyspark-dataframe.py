# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 02 : Basics of Pyspark and Spark Machine Learning
# MAGIC Here we compare **Pandas Dataframe** with **Spark Dataframe** (Spark Dataframe, Pyspark, and MLlib) using a trivial machine learning sample on Databricks, and find that the latter one works as Spark scaling jobs.
# MAGIC
# MAGIC In this exercise, you will learn :
# MAGIC
# MAGIC - Pandas Dataframe with non-Spark commands : It runs only on driver (master) and not distributed.
# MAGIC - Spark Dataframe with Spark commands : It runs as worker jobs (works on executors in worker nodes) in distributed manners.
# MAGIC - Finally, I'll introduce new **pyspark.pandas** (formerly, Koalas), in which you can use familiar pandas syntax with Spark scaling manners. (This exercise needs Databricks Runtime 10.0 or above.)
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 01 : Use pandas Dataframe
# MAGIC
# MAGIC Here we use familiar pandas dataframe with scikit-learn framework.<br>
# MAGIC All operations run on driver and will not be scaled.

# COMMAND ----------

# prepare data
import numpy as np
np.random.seed(0)
x = np.arange(-10, 11)
y = 2*x + 1 + np.random.normal()
l = list(zip(x, y))
l

# COMMAND ----------

# create pandas dataframe
import pandas as pd
df = pd.DataFrame(l, columns=["x","y"])
df

# COMMAND ----------

# pandas dataframe transform
df["x"] = df["x"] + 1
df

# COMMAND ----------

# linear regression with scikit-learn
from sklearn.linear_model import LinearRegression
lr = LinearRegression()
X = df["x"].values.reshape((21,1))
Y = df["y"].values.reshape((21,1))
lr.fit(X, Y)

# COMMAND ----------

# predict
lr.predict([[20]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 02 : Use Spark Dataframe
# MAGIC
# MAGIC Next we will use Spark dataframe and scalable Spark ML libraries.<br>
# MAGIC You will find that operations are invoked as Spark jobs and will be scaled on Spark cluster.

# COMMAND ----------

# create Spark dataframe
from pyspark.sql import Row
rdd = sc.parallelize(l)
rows = rdd.map(lambda z: Row(x=int(z[0]), y=float(z[1])))
df = spark.createDataFrame(rows)
# for viewing data
display(df)

# COMMAND ----------

# Spark dataframe transform
# (The syntax is different from pandas.)
df = df.withColumn("x", df.x + 1)
# for viewing data
display(df)

# COMMAND ----------

# SparkML linear regression
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols = ["x"], outputCol = "features")
va_df = vectorAssembler.transform(df)

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = "features", labelCol="y")
model = lr.fit(va_df)

# COMMAND ----------

# predict using trained model
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

test_schema = StructType([StructField("x", IntegerType())])
test_row = [Row(x=20)]
test_df = spark.createDataFrame(test_row, test_schema)
va_test_df = vectorAssembler.transform(test_df)

pred = model.transform(va_test_df)
test_res = pred.select("x", "prediction")
test_res.cache()
display(test_res)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 03 : Use pyspark.pandas Dataframe
# MAGIC
# MAGIC Finally, we will use pyspark.pandas dataframe.<br>
# MAGIC To run this example, use **Databricks Runtime 10.0 (Spark 3.2) or above**.

# COMMAND ----------

# convert to pyspark.pandas dataframe
from pyspark.sql import Row
rdd = sc.parallelize(l)
rows = rdd.map(lambda z: Row(x=int(z[0]), y=float(z[1])))
df = spark.createDataFrame(rows)
p_df = df.to_pandas_on_spark()
p_df

# Run as follows, when you read data as pyspark.pandas dataframe from file
# from pyspark.pandas import read_csv
# pandas_df = read_csv("data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Once you have got pyspark.pandas dataframe, you can wirte with same syntax as familiar pandas dataframe. (Compare with above code of pandas datafrme.)<br>
# MAGIC The data is processed in distributed manners on Spark executors.

# COMMAND ----------

# pyspark.pandas dataframe transform
# (same as familiar pandas dataframe)
p_df["x"] = p_df["x"] + 1
p_df

# COMMAND ----------

# MAGIC %md
# MAGIC You can also use Spark SQL in pyspark.pandas.

# COMMAND ----------

import pyspark.pandas as ps
ps.sql("SELECT count(*) as num FROM {p_df}")

# COMMAND ----------

# MAGIC %md
# MAGIC By converting to Spark Dataframe, you can interact with the scalable machine learning libraries in Spark.

# COMMAND ----------

# SparkML linear regression
from pyspark.ml.feature import VectorAssembler
df = p_df.to_spark()
vectorAssembler = VectorAssembler(inputCols = ["x"], outputCol = "features")
va_df = vectorAssembler.transform(df)

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = "features", labelCol="y")
model = lr.fit(va_df)

# COMMAND ----------

