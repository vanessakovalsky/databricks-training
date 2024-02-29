# Databricks notebook source
# MAGIC %md
# MAGIC #Use aggregate functions
# MAGIC
# MAGIC ** Data Source **
# MAGIC * English Wikipedia pageviews by second
# MAGIC * Size on Disk: ~255 MB
# MAGIC * Type: Parquet files
# MAGIC * More Info: <a href="https://datahub.io/en/dataset/english-wikipedia-pageviews-by-second" target="_blank">https&#58;//datahub.io/en/dataset/english-wikipedia-pageviews-by-second</a>
# MAGIC
# MAGIC **Technical Accomplishments:**
# MAGIC * Introduce the various aggregate functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Data Source
# MAGIC
# MAGIC This data uses the **Pageviews By Seconds** data set.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# I've already gone through the exercise to determine
# how many partitions I want and in this case it is...
partitions = 8

# Make sure wide operations don't repartition to 200
spark.conf.set("spark.sql.shuffle.partitions", str(partitions))

# COMMAND ----------

(source, sasEntity, sasToken) = getAzureDataSource()
spark.conf.set(sasEntity, sasToken)

# The directory containing our parquet files.
parquetFile = source + "/wikipedia/pageviews/pageviews_by_second.parquet/"

# COMMAND ----------

# Create our initial DataFrame. We can let it infer the 
# schema because the cost for parquet files is really low.
initialDF = (spark.read
  .option("inferSchema", "true") # The default, but not costly w/Parquet
  .parquet(parquetFile)          # Read the data in
  .repartition(partitions)       # From 7 >>> 8 partitions
  .cache()                       # Cache the expensive operation
)
# materialize the cache
initialDF.count()

# rename the timestamp column and cast to a timestamp data type
pageviewsDF = (initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .withColumn("capturedAt", unix_timestamp( col("capturedAt"), "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
)

# cache the transformations on our new DataFrame by marking the DataFrame as cached and then materialize the result
pageviewsDF.cache().count()

# COMMAND ----------

display(pageviewsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) groupBy()
# MAGIC
# MAGIC Aggregating data is one of the more common tasks when working with big data.
# MAGIC * How many customers are over 65?
# MAGIC * What is the ratio of men to women?
# MAGIC * Group all emails by their sender.
# MAGIC
# MAGIC The function `groupBy()` is one tool that we can use for this purpose.
# MAGIC
# MAGIC If you look at the API docs, `groupBy(..)` is described like this:
# MAGIC > Groups the Dataset using the specified columns, so that we can run aggregation on them.
# MAGIC
# MAGIC This function is a **wide** transformation - it will produce a shuffle and conclude a stage boundary.
# MAGIC
# MAGIC Unlike all of the other transformations we've seen so far, this transformation does not return a `DataFrame`.
# MAGIC * In Scala it returns `RelationalGroupedDataset`
# MAGIC * In Python it returns `GroupedData`
# MAGIC
# MAGIC This is because the call `groupBy(..)` is only 1/2 of the transformation.
# MAGIC
# MAGIC To see the other half, we need to take a look at it's return type, `RelationalGroupedDataset`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### RelationalGroupedDataset
# MAGIC
# MAGIC If we take a look at the API docs for `RelationalGroupedDataset`, we can see that it supports the following aggregations:
# MAGIC
# MAGIC | Method | Description |
# MAGIC |--------|-------------|
# MAGIC | `avg(..)` | Compute the mean value for each numeric columns for each group. |
# MAGIC | `count(..)` | Count the number of rows for each group. |
# MAGIC | `sum(..)` | Compute the sum for each numeric columns for each group. |
# MAGIC | `min(..)` | Compute the min value for each numeric column for each group. |
# MAGIC | `max(..)` | Compute the max value for each numeric columns for each group. |
# MAGIC | `mean(..)` | Compute the average value for each numeric columns for each group. |
# MAGIC | `agg(..)` | Compute aggregates by specifying a series of aggregate columns. |
# MAGIC | `pivot(..)` | Pivots a column of the current DataFrame and perform the specified aggregation. |
# MAGIC
# MAGIC With the exception of `pivot(..)`, each of these functions return our new `DataFrame`.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Together, `groupBy(..)` and `RelationalGroupedDataset` (or `GroupedData` in Python) give us what we need to answer some basic questions.
# MAGIC
# MAGIC For Example, how many more requests did the desktop site receive than the mobile site receive?
# MAGIC
# MAGIC For this all we need to do is group all records by **site** and then sum all the requests.

# COMMAND ----------

display(
  pageviewsDF
    .groupBy( col("site") )
    .sum()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice above that we didn't actually specify which column we were summing....
# MAGIC
# MAGIC In this case you will actually receive a total for all numerical values.
# MAGIC
# MAGIC There is a performance catch to that - if I have 2, 5, 10? columns, then they will all be summed and I may only need one.
# MAGIC
# MAGIC I can first reduce my columns to those that I wanted or I can simply specify which column(s) to sum up.

# COMMAND ----------

display(
  pageviewsDF
    .groupBy( col("site") )
    .sum("requests")
)

# COMMAND ----------

# MAGIC %md
# MAGIC And because I don't like the resulting column name, **sum(requests)** I can easily rename it...

# COMMAND ----------

display(
  pageviewsDF
    .groupBy( col("site") )
    .sum("requests")
    .withColumnRenamed("sum(requests)", "totalRequests")
)

# COMMAND ----------

# MAGIC %md
# MAGIC How about the total number of requests per site? mobile vs desktop?

# COMMAND ----------

display(
  pageviewsDF
    .groupBy( col("site") )
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC This result shouldn't surprise us... there were after all one record, per second, per site....

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) sum(), count(), avg(), min(), max()

# COMMAND ----------

# MAGIC %md
# MAGIC The `groupBy(..)` operation is not our only option for aggregating.
# MAGIC
# MAGIC The `...sql.functions` package actually defines a large number of aggregate functions
# MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java
# MAGIC * `pyspark.sql.functions` in the case of Python
# MAGIC
# MAGIC
# MAGIC Let's take a look at this in the Scala API docs (only because the documentation is a little easier to read).

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at our last two examples... 
# MAGIC
# MAGIC We saw the count of records and the sum of records.
# MAGIC
# MAGIC Let's take do this a slightly different way...
# MAGIC
# MAGIC This time with the `...sql.functions` operations.
# MAGIC
# MAGIC And just for fun, let's throw in the average, minimum and maximum

# COMMAND ----------

(pageviewsDF
  .filter("site = 'mobile'")
  .select( sum( col("requests")), count(col("requests")), avg(col("requests")), min(col("requests")), max(col("requests")) )
  .show()
)
          
(pageviewsDF
  .filter("site = 'desktop'")
  .select( sum( col("requests")), count(col("requests")), avg(col("requests")), min(col("requests")), max(col("requests")) )
  .show()
)

# COMMAND ----------

# MAGIC %md
# MAGIC And let's just address one more pet-peeve...
# MAGIC
# MAGIC Was that 3.6M records or 360K records?

# COMMAND ----------

(pageviewsDF
  .filter("site = 'mobile'")
  .select( 
    format_number(sum(col("requests")), 0).alias("sum"), 
    format_number(count(col("requests")), 0).alias("count"), 
    format_number(avg(col("requests")), 2).alias("avg"), 
    format_number(min(col("requests")), 0).alias("min"), 
    format_number(max(col("requests")), 0).alias("max") 
  )
  .show()
)

(pageviewsDF
  .filter("site = 'desktop'")
  .select( 
    format_number(sum(col("requests")), 0), 
    format_number(count(col("requests")), 0), 
    format_number(avg(col("requests")), 2), 
    format_number(min(col("requests")), 0), 
    format_number(max(col("requests")), 0) 
  )
  .show()
)