# Databricks notebook source
# MAGIC %md
# MAGIC # CI/CD Lab
# MAGIC ## Distinct Articles

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) About this Notebook
# MAGIC
# MAGIC In the cell provided below, we count the number of distinct articles in our data set.
# MAGIC
# MAGIC 0. Read in Wikipedia parquet files.
# MAGIC 0. Apply the necessary transformations.
# MAGIC 0. Define a schema that matches the data we are working with.
# MAGIC 0. Assign the count to the variable `totalArticles`

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Retrieve Wikipedia Articles

# COMMAND ----------

(source, sasEntity, sasToken) = getAzureDataSource()
spark.conf.set(sasEntity, sasToken)

path = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

# COMMAND ----------

# Define a schema and load the Parquet files

from pyspark.sql.types import *

parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

schema = StructType([
  StructField("project", StringType(), False),
  StructField("article", StringType(), False),
  StructField("requests", IntegerType(), False),
  StructField("bytes_served", LongType(), False)
])

df = (spark.read
  .schema(schema)
  .parquet(parquetDir)
  .select("*")
  .distinct()
)

totalArticles = df.count()

print("Distinct Articles: {0:,}".format( totalArticles ))

# COMMAND ----------

display(df)

# COMMAND ----------

# display(df.select("article"))