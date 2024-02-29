# Databricks notebook source
# MAGIC %md
# MAGIC # Actions & Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

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

# MAGIC %md-sandbox
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Actions
# MAGIC
# MAGIC In production code, actions will generally **write data to persistent storage** using the DataFrameWriter discussed in other Azure Databricks learning path modules.
# MAGIC
# MAGIC During interactive code development in Databricks notebooks, the `display` method will frequently be used to **materialize a view of the data** after logic has been applied.
# MAGIC
# MAGIC A number of other actions provide the ability to return previews or specify physical execution plans for how logic will map to data. For the complete list, review the [API docs](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset).
# MAGIC
# MAGIC | Method | Return | Description |
# MAGIC |--------|--------|-------------|
# MAGIC | `collect()` | Collection | Returns an array that contains all of Rows in this Dataset. |
# MAGIC | `count()` | Long | Returns the number of rows in the Dataset. |
# MAGIC | `first()` | Row | Returns the first row. |
# MAGIC | `foreach(f)` | - | Applies a function f to all rows. |
# MAGIC | `foreachPartition(f)` | - | Applies a function f to each partition of this Dataset. |
# MAGIC | `head()` | Row | Returns the first row. |
# MAGIC | `reduce(f)` | Row | Reduces the elements of this Dataset using the specified binary function. |
# MAGIC | `show(..)` | - | Displays the top 20 rows of Dataset in a tabular form. |
# MAGIC | `take(n)` | Collection | Returns the first n rows in the Dataset. |
# MAGIC | `toLocalIterator()` | Iterator | Return an iterator that contains all of Rows in this Dataset. |
# MAGIC
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Actions such as `collect` can lead to out of memory errors by forcing the collection of all data.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Transformations
# MAGIC
# MAGIC Transformations have the following key characteristics:
# MAGIC * They eventually return another `DataFrame`.
# MAGIC * They are immutable - that is each instance of a `DataFrame` cannot be altered once it's instantiated.
# MAGIC   * This means other optimizations are possible - such as the use of shuffle files (to be discussed in detail later)
# MAGIC * Are classified as either a Wide or Narrow operation
# MAGIC
# MAGIC Most operations in Spark are **transformations**. While many transformations are [DataFrame operations](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset), writing efficient Spark code will require importing methods from the `sql.functions` module, which contains [transformations corresponding to SQL built-in operations](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Types of Transformations
# MAGIC
# MAGIC A transformation may be wide or narrow.
# MAGIC
# MAGIC A wide transformation requires sharing data across workers. 
# MAGIC
# MAGIC A narrow transformation can be applied per partition/worker with no need to share or shuffle data to other workers. 

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Narrow Transformations
# MAGIC
# MAGIC The data required to compute the records in a single partition reside in at most one partition of the parent Dataframe.
# MAGIC
# MAGIC Examples include:
# MAGIC * `filter(..)`
# MAGIC * `drop(..)`
# MAGIC * `coalesce()`
# MAGIC
# MAGIC ![](https://databricks.com/wp-content/uploads/2018/05/Narrow-Transformation.png)

# COMMAND ----------

from pyspark.sql.functions import col

display(countsDF.filter(col("NAME").like("%TX%")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Wide Transformations
# MAGIC
# MAGIC The data required to compute the records in a single partition may reside in many partitions of the parent Dataframe. These operations require that data is **shuffled** between executors.
# MAGIC
# MAGIC Examples include:
# MAGIC * `distinct()`
# MAGIC * `groupBy(..).sum()`
# MAGIC * `repartition(n)`
# MAGIC
# MAGIC ![](https://databricks.com/wp-content/uploads/2018/05/Wide-Transformation.png)

# COMMAND ----------

display(countsDF.groupBy("UNIT").sum("counts"))