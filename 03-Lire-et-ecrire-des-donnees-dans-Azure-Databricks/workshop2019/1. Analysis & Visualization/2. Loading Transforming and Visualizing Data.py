# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Loading, Transforming and Visualizing Data with Spark and Azure Databricks
# MAGIC
# MAGIC This notebook shows how to create and query a table or DataFrame on Azure Blob Storage, and use visualisations on the data.
# MAGIC
# MAGIC The data used in this example is taken from the [MovieLens dataset](https://grouplens.org/datasets/movielens/).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 1: Set the data location
# MAGIC
# MAGIC There are two ways in Databricks to read from Azure Blob Storage. You can either read data using [account keys](https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html#azure-blob-storage) or read data using shared access signatures (SAS).
# MAGIC
# MAGIC To get started, we need to set the location and type of the files. We can do this using [widgets](https://docs.databricks.com/user-guide/notebooks/widgets.html). Widgets allow us to parameterize the execution of this entire notebook. First we create them, then we can reference them throughout the notebook. 
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text("container_name", "sample-container", "Container name")
dbutils.widgets.text("storage_account_access_key", "YOUR_ACCESS_KEY", "Storage Access Key / SAS")
dbutils.widgets.text("storage_account_name", "STORAGE_ACCOUNT_NAME", "Storage Account Name")

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+dbutils.widgets.get("storage_account_name")+".blob.core.windows.net",
  dbutils.widgets.get("storage_account_access_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Read the data
# MAGIC
# MAGIC Now that we have specified our file metadata, we can create a [DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes). Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC
# MAGIC Also, since we are reading files from Azure Blob Storage, we'll be using the [WASBS protocol](https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html) to refer to their location.
# MAGIC
# MAGIC First, let's create a couple of DataFrames in Python, to work with our *Movies* and *Reviews* data.

# COMMAND ----------

from pyspark.sql.functions import split, explode, col, udf
from pyspark.sql.types import *

ratingsLocation = "wasbs://"+dbutils.widgets.get("container_name")+"@" +dbutils.widgets.get("storage_account_name")+".blob.core.windows.net/ratings.csv"
moviesLocation = "wasbs://"+dbutils.widgets.get("container_name")+"@" +dbutils.widgets.get("storage_account_name")+".blob.core.windows.net/movies.csv"

print("Ratings file location       " + ratingsLocation)
print("Movies file location        " + moviesLocation)

ratings = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(ratingsLocation)

movies = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(moviesLocation)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Query the data
# MAGIC
# MAGIC Now that we have created our DataFrames, we can query them. For instance, you can identify particular columns to select and display within Databricks.

# COMMAND ----------

display(ratings)

# COMMAND ----------

display(movies)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We'll perform a little data processing using `pyspark.sql`, which is the Spark SQL module for Python.

# COMMAND ----------

# transform the timestamp data column to a date column
# first we cast the int column to Timestamp
ratingsTemp = ratings \
  .withColumn("ts", ratings.timestamp.cast("Timestamp")) 
  
# then, we cast Timestamp to Date
ratings = ratingsTemp \
  .withColumn("reviewDate", ratingsTemp.ts.cast("Date")) \
  .drop("ts", "timestamp")

# COMMAND ----------

# use a Spark UDF(user-defined function) to get the year a movie was made, from the title
# NOTE: since Spark UDFs can impact performance, it's a good idea to persist the result to disk, to avoid having to re-do the computation
def titleToYear(title):
  try:
    return int(title[title.rfind("(")+1:title.rfind(")")])
  except:
    return None

# register the above Spark function as UDF
titleToYearUdf = udf(titleToYear, IntegerType())

# add the movieYear column
movies = movies.withColumn("movieYear", titleToYearUdf(movies.title))

# explode the 'movies'.'genres' values into separate rows
movies_denorm = movies.withColumn("genre", explode(split("genres", "\|"))).drop("genres")

# join movies and ratings datasets on movieId
ratings_denorm = ratings.alias('a').join(movies_denorm.alias('b'), 'movieId', 'inner') 

display(ratings_denorm)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC
# MAGIC If you want to query this data as a table using SQL syntax, you can simply register it as a *view* or a table.

# COMMAND ----------

ratings.createOrReplaceTempView("ratings")
movies.createOrReplaceTempView("movies")
ratings_denorm.createOrReplaceTempView("ratings_denorm")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can query a registered temporary view using Spark SQL. Notice how we can use `%sql` to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT movieId, userId, rating, reviewDate
# MAGIC FROM ratings

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This would be equivalent to using the `spark.sql()` function call in Python:

# COMMAND ----------

result = spark.sql("SELECT movieId, userId, rating, reviewDate FROM ratings")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can also use SQL aggregate functions like `COUNT`, `SUM`, or `AVG`.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT genre, COUNT(rating) as total_ratings, AVG(rating) AS average_rating 
# MAGIC FROM ratings_denorm
# MAGIC GROUP BY genre
# MAGIC ORDER BY total_ratings DESC;

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC We can see the execution plan of that last query by using the `EXPLAIN` keyword.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC EXPLAIN
# MAGIC SELECT genre, COUNT(rating) as total_ratings, AVG(rating) AS average_rating 
# MAGIC FROM ratings_denorm
# MAGIC GROUP BY genre
# MAGIC ORDER BY total_ratings DESC;

# COMMAND ----------

# MAGIC %md ### Step 5: Visualizations
# MAGIC
# MAGIC The easiest way to create a visualization in Databricks is either to call `display(<dataframe-name>)`, or to simply run a SQL command as described above. Then you can  click the chart icon ![](https://docs.databricks.com/_images/chart-button.png) below the results to display a chart. 
# MAGIC
# MAGIC You can also click the down arrow next to the bar chart ![](https://docs.databricks.com/_images/chart-button.png) to choose another chart type and click **Plot Options...** to configure the chart.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT genre, COUNT(rating) as total_ratings, AVG(rating) AS average_rating 
# MAGIC FROM ratings_denorm
# MAGIC GROUP BY genre
# MAGIC ORDER BY average_rating DESC;

# COMMAND ----------

# MAGIC %md Let's try some more visualizations. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT rating from ratings

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT movieYear, genre, COUNT(*) as n FROM ratings_denorm 
# MAGIC GROUP BY movieYear, genre

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT title, COUNT(rating) as total_ratings, AVG(rating) AS average_rating 
# MAGIC FROM ratings_denorm
# MAGIC GROUP BY title
# MAGIC HAVING total_ratings > 30

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 6: Persist the data for future use
# MAGIC
# MAGIC Registered as a temp view, this data is only available to this particular notebook. If you want other users to be able to query this table, you can also create a table from the DataFrame.

# COMMAND ----------

ratings.write.saveAsTable('ratings', format='parquet', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This table will persist across cluster restarts and allow various users across different notebooks to query this data
# MAGIC
# MAGIC You can check out the saved table in the **Data** node on the left side of the screen.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Exercises
# MAGIC
# MAGIC Using either Spark SQL or the Databricks visualization tools (or both), try to answer the following questions:
# MAGIC
# MAGIC 1. How many movies were produced per year?
# MAGIC 2. What were the best movies of every decade (based on users’ ratings)?