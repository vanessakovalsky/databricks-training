# Databricks notebook source
# MAGIC %md
# MAGIC #Perform date and time manipulation
# MAGIC
# MAGIC ** Data Source **
# MAGIC * English Wikipedia pageviews by second
# MAGIC * Size on Disk: ~255 MB
# MAGIC * Type: Parquet files
# MAGIC * More Info: <a href="https://datahub.io/en/dataset/english-wikipedia-pageviews-by-second" target="_blank">https&#58;//datahub.io/en/dataset/english-wikipedia-pageviews-by-second</a>
# MAGIC
# MAGIC **Technical Accomplishments:**
# MAGIC * Explore more of the `...sql.functions` operations
# MAGIC   * Date & time functions

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

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Preparing Our Data
# MAGIC
# MAGIC If we will be working on any given dataset for a while, there are a handful of "necessary" steps to get us ready...
# MAGIC
# MAGIC Most of which we've just knocked out above.
# MAGIC
# MAGIC **Basic Steps**
# MAGIC 0. <div style="text-decoration:line-through">Read the data in</div>
# MAGIC 0. <div style="text-decoration:line-through">Balance the number of partitions to the number of slots</div>
# MAGIC 0. <div style="text-decoration:line-through">Cache the data</div>
# MAGIC 0. <div style="text-decoration:line-through">Adjust the `spark.sql.shuffle.partitions`</div>
# MAGIC 0. Perform some basic ETL (i.e., convert strings to timestamp)
# MAGIC 0. Possibly re-cache the data if the ETL was costly
# MAGIC
# MAGIC What we haven't done is some of the basic ETL necessary to explore our data.
# MAGIC
# MAGIC Namely, the problem is that the field "timestamp" is a string.
# MAGIC
# MAGIC In order to performed date/time - based computation I need to convert this to an alternate datetime format.

# COMMAND ----------

initialDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) withColumnRenamed(..), withColumn(..), select(..)
# MAGIC
# MAGIC My first hangup is that we have a **column named timestamp** and the **datatype will also be timestamp**
# MAGIC
# MAGIC The nice thing about Apache Spark is that I'm allowed the have an issue with this because it's very easy to fix...
# MAGIC
# MAGIC Just rename the column...

# COMMAND ----------

(initialDF
  .select( col("timestamp").alias("capturedAt"), col("site"), col("requests") )
  .printSchema()
)

# COMMAND ----------

# MAGIC %md
# MAGIC There are a number of different ways to rename a column...

# COMMAND ----------

(initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .printSchema()
)

# COMMAND ----------

(initialDF
  .toDF("capturedAt", "site", "requests")
  .printSchema()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) unix_timestamp(..) & cast(..)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that **we** are over **my** hangup, we can focus on converting the **string** to a **timestamp**.
# MAGIC
# MAGIC For this we will be looking at more of the functions in the `functions` package
# MAGIC * `pyspark.sql.functions` in the case of Python
# MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java
# MAGIC
# MAGIC And so that we can watch the transformation, will will take one step at a time...

# COMMAND ----------

# MAGIC %md
# MAGIC The first function is `unix_timestamp(..)`
# MAGIC
# MAGIC If you look at the API docs, `unix_timestamp(..)` is described like this:
# MAGIC > Convert time string with given pattern (see <a href="http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html" target="_blank">SimpleDateFormat</a>) to Unix time stamp (in seconds), return null if fail.
# MAGIC
# MAGIC `SimpleDataFormat` is part of the Java API and provides support for parsing and formatting date and time values.
# MAGIC
# MAGIC In order to know what format the data is in, let's take a look at the first row...

# COMMAND ----------

# MAGIC %md
# MAGIC Comparing that value with the patterns express in the docs for the `SimpleDateFormat` class, we can come up with a format:
# MAGIC
# MAGIC **yyyy-MM-dd HH:mm:ss**

# COMMAND ----------

tempA = (initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .select( col("*"), unix_timestamp( col("capturedAt"), "yyyy-MM-dd HH:mm:ss") )
)
tempA.printSchema()

# COMMAND ----------

display(tempA)

# COMMAND ----------

# MAGIC %md
# MAGIC ** *Note:* ** *If you haven't caught it yet, there is a bug in the previous code....*

# COMMAND ----------

# MAGIC %md
# MAGIC A couple of things happened...
# MAGIC 0. We ended up with a new column - that's OK for now
# MAGIC 0. The new column has a really funky name - based upon the name of the function we called and its parameters.
# MAGIC 0. The data type is now a long.
# MAGIC   * This value is the Java Epoch
# MAGIC   * The number of seconds since 1970-01-01T00:00:00Z
# MAGIC   
# MAGIC We can now take that epoch value and use the `Column.cast(..)` method to convert it to a **timestamp**.

# COMMAND ----------

tempB = (initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .select( col("*"), unix_timestamp( col("capturedAt"), "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
)
tempB.printSchema()

# COMMAND ----------

display(tempB)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that our column `createdAt` has been converted from a **string** to a **timestamp**, we just need to deal with this REALLY funky column name.
# MAGIC
# MAGIC Again.. there are several ways to do this.
# MAGIC
# MAGIC I'll let you decide which you like better...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option #1
# MAGIC The `as()` or `alias()` method can be appended to the chain of calls.
# MAGIC
# MAGIC This version will actually produce an odd little bug.<br/>
# MAGIC That is, how do you get rid of only one of the two `capturedAt` columns?

# COMMAND ----------

tempC = (initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .select( col("*"), unix_timestamp( col("capturedAt"), "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp").alias("capturedAt") )
)
tempC.printSchema()

# COMMAND ----------

display(tempC)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option #2
# MAGIC The `withColumn(..)` renames the column (first param) and accepts as a<br/>
# MAGIC second parameter the expression(s) we need for our transformation

# COMMAND ----------

tempD = (initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .withColumn("capturedAt", unix_timestamp( col("capturedAt"), "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
)
tempD.printSchema()

# COMMAND ----------

display(tempD)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option #3
# MAGIC
# MAGIC We can take the big ugly name explicitly rename it.
# MAGIC
# MAGIC This version will actually produce an odd little bug.<br/>
# MAGIC That is how do you get rid of only one of the two "capturedAt" columns?

# COMMAND ----------

#Option #3

tempE = (initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .select( col("*"), unix_timestamp( col("capturedAt"), "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
  .withColumnRenamed("CAST(unix_timestamp(capturedAt, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "capturedAt")
  # .drop("timestamp")
)
tempE.printSchema()

# COMMAND ----------

display(tempE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option #4
# MAGIC
# MAGIC The last version is a twist on the others in which we start with the <br/>
# MAGIC name `timestamp` and rename it and the expression all in one call<br/>
# MAGIC
# MAGIC But this version leaves us with the old column in the DF

# COMMAND ----------

tempF = (initialDF
  .withColumn("capturedAt", unix_timestamp( col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
)
tempF.printSchema()

# COMMAND ----------

display(tempF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's pick the "cleanest" version...
# MAGIC
# MAGIC And with our base `DataFrame` in place we can start exploring the data a little...

# COMMAND ----------

pageviewsDF = (initialDF
  .withColumnRenamed("timestamp", "capturedAt")
  .withColumn("capturedAt", unix_timestamp( col("capturedAt"), "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
)

pageviewsDF.printSchema()

# COMMAND ----------

display(pageviewsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC And just so that we don't have to keep performing these transformations.... 
# MAGIC
# MAGIC Mark the `DataFrame` as cached and then materialize the result.

# COMMAND ----------

pageviewsDF.cache().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) year(..), month(..), dayofyear(..)
# MAGIC
# MAGIC Let's take a look at some of the other date & time functions...
# MAGIC
# MAGIC With that we can answer a simple question: When was this data captured.
# MAGIC
# MAGIC We can start specifically with the year...

# COMMAND ----------

display(
  pageviewsDF
    .select( year( col("capturedAt")) ) # Every record converted to a single column - the year captured
    .distinct()                         # Reduce all years to the list of distinct years
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now let's take a look at in which months was this data captured...

# COMMAND ----------

display(
  pageviewsDF
    .select( month( col("capturedAt")) ) # Every record converted to a single column - the month captured
    .distinct()                          # Reduce all months to the list of distinct months
)

# COMMAND ----------

# MAGIC %md
# MAGIC And of course this both can be combined as a single call...

# COMMAND ----------

(pageviewsDF
  .select( month(col("capturedAt")).alias("month"), year(col("capturedAt")).alias("year"))
  .distinct()
  .show()                     
)

# COMMAND ----------

# MAGIC %md
# MAGIC It's pretty easy to see that the data was captured during March & April of 2015.
# MAGIC
# MAGIC We will have more opportunities to play with the various date and time functions in the next exercise.
# MAGIC
# MAGIC For now, let's just make sure to review them in the Spark API

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC Start the next lesson, [Use aggregate functions]($./2.Use-Aggregate-Functions)