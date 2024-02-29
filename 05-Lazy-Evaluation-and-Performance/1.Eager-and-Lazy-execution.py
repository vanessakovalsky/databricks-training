# Databricks notebook source
# MAGIC %md
# MAGIC # Describe the difference between eager and lazy execution

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Laziness By Design
# MAGIC
# MAGIC Fundamental to Apache Spark are the notions that
# MAGIC * Transformations are **LAZY**
# MAGIC * Actions are **EAGER**
# MAGIC
# MAGIC The following code condenses the logic from the DataFrames modules in this learning path, and uses the DataFrames API to:
# MAGIC - Specify a schema, format, and file source for the data to be loaded
# MAGIC - Select columns to `GROUP BY`
# MAGIC - Aggregate with a `COUNT`
# MAGIC - Provide an alias name for the aggregate output
# MAGIC - Specify a column to sort on
# MAGIC
# MAGIC This cell defines a series of **transformations**. By definition, this logic will result in a DataFrame and will not trigger any jobs.

# COMMAND ----------

schemaDDL = "NAME STRING, STATION STRING, LATITUDE FLOAT, LONGITUDE FLOAT, ELEVATION FLOAT, DATE DATE, UNIT STRING, TAVG FLOAT"

sourcePath = "/mnt/training/weather/StationData/stationData.parquet/"

countsDF = (spark.read
  .format("parquet")
  .schema(schemaDDL)
  .load(sourcePath)
  .groupBy("NAME", "UNIT").count()
  .withColumnRenamed("count", "counts")
  .orderBy("NAME")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Because `display` is an **action**, a job _will_ be triggered, as logic is executed against the specified data to return a result.

# COMMAND ----------

display(countsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why is Laziness So Important?
# MAGIC
# MAGIC Laziness is at the core of Scala and Spark.
# MAGIC
# MAGIC It has a number of benefits:
# MAGIC * Not forced to load all data at step #1
# MAGIC   * Technically impossible with **REALLY** large datasets.
# MAGIC * Easier to parallelize operations
# MAGIC   * N different transformations can be processed on a single data element, on a single thread, on a single machine.
# MAGIC * Optimizations can be applied prior to code compilation