# Databricks notebook source
# MAGIC %md
# MAGIC # Reading and writing from Azure Cosmos DB
# MAGIC
# MAGIC **In this lesson you:**
# MAGIC - Write data into Azure Cosmos DB
# MAGIC - Read data from Azure Cosmos DB
# MAGIC
# MAGIC ## Library Requirements
# MAGIC
# MAGIC 1. the Maven library with coordinate `com.databricks.training:databricks-cosmosdb-spark2.2.0-scala2.11:1.0.0` in the `https://files.training.databricks.com/repo` repository.
# MAGIC    - this allows a Databricks `spark` session to communicate with Azure Cosmos DB
# MAGIC
# MAGIC The next cell walks you through installing the Maven library.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Lab Setup
# MAGIC
# MAGIC If you are running in an Azure Databricks environment that is already pre-configured with the libraries you need, you can skip to the next cell. To use this notebook in your own Databricks environment, you will need to create libraries, using the [Create Library](https://docs.azuredatabricks.net/user-guide/libraries.html) interface in Azure Databricks. Follow the steps below to attach the `azure-cosmosdb-spark` library to your cluster:
# MAGIC
# MAGIC %md
# MAGIC 1. Right click on the browser tab and select "Duplicate" to open a new tab.
# MAGIC 1. In the left-hand navigation menu of your Databricks workspace, select **Clusters**, then select your cluster in the list. If it's not running, start it now.
# MAGIC
# MAGIC   ![Select cluster](https://databricksdemostore.blob.core.windows.net/images/10-de-learning-path/select-cluster.png)
# MAGIC
# MAGIC 1. Select the **Libraries** tab (1), then select **Install New** (2). In the Install Library dialog, select **Maven** under Library Source (3). Under Coordinates, paste `com.databricks.training:databricks-cosmosdb-spark2.2.0-scala2.11:1.0.0` (4). Under Repository, paste `https://files.training.databricks.com/repo` (5), then select **Install** (6).
# MAGIC   
# MAGIC   ![Databricks new Maven library](https://databricksdemostore.blob.core.windows.net/images/14-de-learning-path/install-cosmosdb-spark-library.png)
# MAGIC
# MAGIC 1. Wait until the library successfully installs before continuing.
# MAGIC
# MAGIC Once complete, return to this notebook to continue with the lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/wiki-book/general/logo_spark_tiny.png) Load Azure Cosmos DB
# MAGIC
# MAGIC Now load a small amount of data into Azure Cosmos DB to demonstrate that connection.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC Enter your Azure Cosmos DB account information in the cell below. Be sure to replace the **"cosmos-db-uri"** and **"your-cosmos-db-key"** values with your own before executing.

# COMMAND ----------

URI = "cosmos-db-uri"
PrimaryKey = "your-cosmos-db-key"

# COMMAND ----------

# MAGIC %md
# MAGIC <span>1.</span> Enter the Azure Cosmos DB connection information into the cell below. <br>

# COMMAND ----------

CosmosDatabase = "AdventureWorks"
CosmosCollection = "ratings"

cosmosConfig = {
  "Endpoint": URI,
  "Masterkey": PrimaryKey,
  "Database": CosmosDatabase,
  "Collection": CosmosCollection,
  "Upsert": "false"
}

# COMMAND ----------

# MAGIC %md
# MAGIC <span>2.</span> Read the input parquet file.

# COMMAND ----------

from pyspark.sql.functions import col
ratingsDF = (spark.read
  .parquet("dbfs:/mnt/training/initech/ratings/ratings.parquet/")
  .withColumn("rating", col("rating").cast("double")))
print("Num Rows: {}".format(ratingsDF.count()))

# COMMAND ----------

display(ratingsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC <span>3.</span> Write the data to Azure Cosmos DB.

# COMMAND ----------

ratingsSampleDF = ratingsDF.sample(.0001)

(ratingsSampleDF.write
  .mode("overwrite")
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**cosmosConfig)
  .save())

# COMMAND ----------

# MAGIC %md
# MAGIC <span>4.</span> Confirm that your data is now in Azure Cosmos DB.

# COMMAND ----------

dfCosmos = (spark.read
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**cosmosConfig)
  .load())

display(dfCosmos)
