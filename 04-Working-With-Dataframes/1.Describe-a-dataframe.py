# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Describe a DataFrame
# MAGIC
# MAGIC Your data processing in Azure Databricks is accomplished by defining Dataframes to read and process the Data.
# MAGIC
# MAGIC This notebook will introduce how to read your data using Azure Databricks Dataframes.

# COMMAND ----------

# MAGIC %md
# MAGIC #Introduction
# MAGIC
# MAGIC ** Data Source **
# MAGIC * One hour of Pagecounts from the English Wikimedia projects captured August 5, 2016, at 12:00 PM UTC.
# MAGIC * Size on Disk: ~23 MB
# MAGIC * Type: Compressed Parquet File
# MAGIC * More Info: <a href="https://dumps.wikimedia.org/other/pagecounts-raw" target="_blank">Page view statistics for Wikimedia projects</a>
# MAGIC
# MAGIC **Technical Accomplishments:**
# MAGIC * Develop familiarity with the `DataFrame` APIs
# MAGIC * Introduce the classes...
# MAGIC   * `SparkSession`
# MAGIC   * `DataFrame` (aka `Dataset[Row]`)
# MAGIC * Introduce the actions...
# MAGIC   * `count()`

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) **The Data Source**
# MAGIC
# MAGIC * In this notebook, we will be using a compressed parquet "file" called **pagecounts** (~23 MB file from Wikipedia)
# MAGIC * We will explore the data and develop an understanding of it as we progress.
# MAGIC * You can read more about this dataset here: <a href="https://dumps.wikimedia.org/other/pagecounts-raw/" target="_blank">Page view statistics for Wikimedia projects</a>.
# MAGIC
# MAGIC We can use **dbutils.fs.ls()** to view our data on the DBFS.

# COMMAND ----------

path = "dbfs:/databricks-datasets/wikipedia-datasets/data-001/pagecounts/sample/"
files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Vous pouvez voir qu'il y a un seul fichier, celui-ci est au format text sans extension, nous allons le traiter comme un fichier CSV

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Create a DataFrame
# MAGIC * We can read the Parquet files into a `DataFrame`.
# MAGIC * We'll start with the object **spark**, an instance of `SparkSession` and the entry point to Spark 2.0 applications.
# MAGIC * From there we can access the `read` object which gives us an instance of `DataFrameReader`.

# COMMAND ----------

file = "dbfs:/databricks-datasets/wikipedia-datasets/data-001/pagecounts/sample/pagecounts-20151124-170000"

# COMMAND ----------

pagecountsEnAllDF = (spark  # Our SparkSession & Entry Point
  .read                     # Our DataFrameReader     
  .csv(file)      # Returns an instance of DataFrame
)
print(pagecountsEnAllDF)    # Python hack to see the data type

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) count()
# MAGIC
# MAGIC If you look at the API docs, `count()` is described like this:
# MAGIC > Returns the number of rows in the Dataset.
# MAGIC
# MAGIC `count()` will trigger a job to process the request and return a value.
# MAGIC
# MAGIC We can now count all records in our `DataFrame` like this:

# COMMAND ----------

total = pagecountsEnAllDF.count()

print("Record Count: {0:,}".format( total ))

# COMMAND ----------

# MAGIC %md
# MAGIC That tells us that there are around 7 million rows in the `DataFrame`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC Start the next lesson, [Use common DataFrame methods]($./2.Use-common-dataframe-methods)