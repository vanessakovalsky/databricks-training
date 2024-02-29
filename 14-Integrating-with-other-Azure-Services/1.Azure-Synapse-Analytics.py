# Databricks notebook source
# MAGIC %md
# MAGIC # Reading and Writing to Azure Synapse Analytics
# MAGIC **Technical Accomplishments:**
# MAGIC - Access an Azure Synapse Analytics warehouse using the SQL Data Warehouse connector
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Databricks Runtime 4.0 or above
# MAGIC - A database master key for Azure Synapse Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Azure Synapse Analytics
# MAGIC Azure Synapse Analytics leverages massively parallel processing (MPP) to quickly run complex queries across petabytes of data.
# MAGIC
# MAGIC Import big data into Azure Synapse Analytics with simple PolyBase T-SQL queries, and then use MPP to run high-performance analytics.
# MAGIC
# MAGIC As you integrate and analyze, the data warehouse will become the single version of truth your business can count on for insights.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) SQL Data Warehouse Connector
# MAGIC
# MAGIC - Use Azure Blob Storage as an intermediary between Azure Databricks and Azure Synapse Analytics
# MAGIC - In Azure Databricks: triggers Spark jobs to read and write data to Blob Storage
# MAGIC - In Azure Synapse Analytics: triggers data loading and unloading operations, performed by **PolyBase**
# MAGIC
# MAGIC **Note:** The SQL DW connector is more suited to ETL than to interactive queries.  
# MAGIC For interactive and ad-hoc queries, data should be extracted into a Databricks Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![Azure Databricks and Synapse Analytics](https://databricksdemostore.blob.core.windows.net/images/14-de-learning-path/databricks-synapse.png)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Types of Connections in Azure Synapse Analytics
# MAGIC
# MAGIC ### **Spark Driver to Azure Synapse Analytics**
# MAGIC Spark driver connects to Azure Synapse Analytics via JDBC using a username and password.
# MAGIC
# MAGIC ### **Spark Driver and Executors to Azure Blob Storage**
# MAGIC Spark uses the **Azure Blob Storage connector** bundled in Databricks Runtime to connect to the Blob Storage container.
# MAGIC   - Requires **`wasbs`** URI scheme to specify connection
# MAGIC   - Requires **storage account access key** to set up connection
# MAGIC     - Set in a notebook's session configuration, which doesn't affect other notebooks attached to the same cluster
# MAGIC     - **`spark`** is the SparkSession object provided in the notebook
# MAGIC
# MAGIC ### **Azure Synapse Analytics to Azure Blob Storage**
# MAGIC SQL DW connector forwards the access key from notebook session configuration to an Azure Synapse Analytics instance over JDBC.
# MAGIC   - Requires **`forwardSparkAzureStorageCredentials`** set to **`true`**
# MAGIC   - Represents access key with a temporary <a href="https://docs.microsoft.com/en-us/sql/t-sql/statements/create-database-scoped-credential-transact-sql?view=sql-server-2017" target="_blank">database scoped credential</a> in the Azure Synapse Analytics instance
# MAGIC   - Creates a database scoped credential before asking Azure Synapse Analytics to load or unload data, and deletes after loading/unloading is finished

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Enabling access for a notebook session

# COMMAND ----------

# MAGIC %md
# MAGIC You can enable access for the lifetime of your notebook session to SQL Data Warehouse by executing the cell below. Be sure to replace the **"name-of-your-storage-account"** and **"your-storage-key"** values with your own before executing.

# COMMAND ----------

storage_account_name = "name-of-your-storage-account"
storage_account_key = "your-storage-key"
storage_container_name = "data"

# COMMAND ----------

# MAGIC %md
# MAGIC You will need the JDBC connection string for your Azure Synapse Analytics service. You should copy this value exactly as it appears in the Azure Portal.
# MAGIC
# MAGIC **Paste your JDBC connection string** into the empty quotation marks below. Please make sure you have replaced `{your_password_here}` with your SQL Server password.

# COMMAND ----------

jdbcURI = ""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Read from the Customer Table
# MAGIC
# MAGIC Use the SQL DW Connector to read data from the Customer Table.
# MAGIC
# MAGIC Use the read to define a tempory table that can be queried.
# MAGIC
# MAGIC Note the following options in the DataFrameReader in the cell below:
# MAGIC * **`url`** specifies the JDBC connection to Azure Synapse Analytics
# MAGIC * **`tempDir`** specifies the **`wasbs`** URI of the caching directory on the Azure Blob Storage container
# MAGIC * **`forwardSparkAzureStorageCredentials`** is set to **`true`** to ensure that the Azure storage account access keys are forwarded from the notebook's session configuration to the Azure Synapse Analytics

# COMMAND ----------

cacheDir = "wasbs://{}@{}.blob.core.windows.net/cacheDir".format(storage_container_name, storage_account_name)

spark_config_key = "fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name)
spark_config_value = storage_account_key

spark.conf.set(spark_config_key, spark_config_value)

tableName = "dbo.DimCustomer"

customerDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName)
  .load())

customerDF.createOrReplaceTempView("customer_data")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Use SQL queries to count the number of rows in the Customer table and to display table metadata.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from customer_data

# COMMAND ----------

# MAGIC %sql
# MAGIC describe customer_data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Note that **`CustomerKey`** and **`CustomerAlternateKey`** use a very similar naming convention.

# COMMAND ----------

# MAGIC %sql
# MAGIC select CustomerKey, CustomerAlternateKey from customer_data limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC When merging many new customers into this table, we may have issues with uniqueness in the **`CustomerKey`**. 
# MAGIC
# MAGIC Let's redefine **`CustomerAlternateKey`** for stronger uniqueness using a <a href="https://en.wikipedia.org/wiki/Universally_unique_identifier" target="_blank">UUID</a>. To do this, we will define a UDF and use it to transform the **`CustomerAlternateKey`** column.

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

uuidUdf = udf(lambda : str(uuid.uuid4()), StringType())
customerUpdatedDF = customerDF.withColumn("CustomerAlternateKey", uuidUdf())
display(customerUpdatedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Use the Polybase Connector to Write to the Staging Table
# MAGIC
# MAGIC Use the SQL DW Connector to write the updated customer table to a staging table.
# MAGIC
# MAGIC It is best practice to update Azure Synapse Analytics via a staging table.
# MAGIC
# MAGIC Note the following options in the DataFrameWriter in the cell below:
# MAGIC * **`url`** specifies the JDBC connection to Azure Synapse Analytics
# MAGIC * **`tempDir`** specifies the **`wasbs`** URI of the caching directory on the Azure Blob Storage container
# MAGIC * **`forwardSparkAzureStorageCredentials`** is set to **`true`** to ensure that the Azure storage account access keys are forwarded from the notebook's session configuration to Azure Synapse Analytics
# MAGIC
# MAGIC These options are the same as those in the DataFrameReader above.

# COMMAND ----------

(customerUpdatedDF.write
  .format("com.databricks.spark.sqldw")
  .mode("overwrite")
  .option("url", jdbcURI)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbtable", tableName + "Staging")
  .option("tempdir", cacheDir)
  .save())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Read From the New Staging Table
# MAGIC Use the SQL DW Connector to read the new table we just wrote.
# MAGIC
# MAGIC Use the read to define a tempory table that can be queried.

# COMMAND ----------

customerTempDF = (spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", jdbcURI)
  .option("tempDir", cacheDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", tableName + "Staging")
  .load())

customerTempDF.createOrReplaceTempView("customer_temp_data")


# COMMAND ----------

# MAGIC %sql
# MAGIC select CustomerKey, CustomerAlternateKey from customer_temp_data limit 10;