# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 01 : Storage Settings (Prepare)
# MAGIC
# MAGIC With Azure Databricks, you can work with various kind of surrounding Azure services, such as Blob, Data Lake Storage, Cosmos DB, SQL Data Warehouse, Power BI, and so on and so forth.
# MAGIC
# MAGIC Here we see how to interact with **Azure Data Lake Storage (Gen2)**.
# MAGIC - Azure Data Lake Storage Gen2 requires Databricks Runtime 4.2 or above
# MAGIC - Azure Data Lake Storage Gen2 mount points require Databricks Runtime 5.1 or above
# MAGIC
# MAGIC Azure Databricks has built-in Databricks File System (DBFS) and files in DBFS persist to Azure Blob storage (which is a locked resource in your subscription), so you won’t lose data even after you terminate a cluster.    
# MAGIC Here we setup your own storage using Azure Data Lake Storage Gen2 for the following exercises.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation
# MAGIC
# MAGIC Before starting this exercise :
# MAGIC 1. Create Azure Storage account in Azure Portal
# MAGIC     - Select "StorageV2 (general purpose v2)" in "Account kind" section
# MAGIC     - Enable "Data Lake Storage Gen2" in "Advanced" tab
# MAGIC 2. Create container (file system) in Data Lake Gen2 File systems (Use latest **Azure Storage Explorer**.)
# MAGIC 3. Upload [flight_weather.csv](https://1drv.ms/u/s!AuopXnMb-AqcgbZD7jEX6OTb4j8CTQ?e=KkeDdT) in your container
# MAGIC 4. Create service principal and set permissions (See "[Use Azure REST API without interactive Login UI with app key or certificate programmatically](https://tsmatz.wordpress.com/2017/03/03/azure-rest-api-with-certificate-and-service-principal-by-silent-backend-daemon-service/)" for details.)
# MAGIC     - Register new app (service principal) in Azure Active Directory    
# MAGIC     (Select "Azure Active Directory" menu in Azure Portal and select "App registrations" to proceed.)
# MAGIC     - In a new application, generate new client secret
# MAGIC     - In your storage account, assign "**Storage Blob Data Contributor**" role for above service principal (application)    
# MAGIC     (Select "Access control (IAM)" menu in your storage account and add new role assignment.)
# MAGIC     - Copy application id (client id) and secret in service principal (application)    
# MAGIC     (These values are used in the following exercise.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 1 : Mount with service principal (secret)
# MAGIC
# MAGIC You can mount Azure blob container and access with ordinary path expression.

# COMMAND ----------

# MAGIC %md
# MAGIC Please set (change) the following values
# MAGIC
# MAGIC source : ```abfss://YOUR_CONTAINER_NAME@YOUR_STORAGE_ACCOUNT_NAME.dfs.core.windows.net/```    
# MAGIC fs.azure.account.oauth2.client.id : ```SERVICE_PRINCIPAL_CLIENT_ID```    
# MAGIC fs.azure.account.oauth2.client.secret : ```SERVICE_PRINCIPAL_CLIENT_SECRET```    
# MAGIC fs.azure.account.oauth2.client.endpoint : ```https://login.microsoftonline.com/YOUR_DIRECTORY_DOMAIN/oauth2/token```    

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://container01@demostore01.dfs.core.windows.net/",
  mount_point = "/mnt/testblob",
  extra_configs = {"fs.azure.account.auth.type": "OAuth",
                   "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                   "fs.azure.account.oauth2.client.id": "31d00ab6-b48d-46eb-a540-dc6ce8cfbcd8",
                   "fs.azure.account.oauth2.client.secret": "!(>d..SaM/...",
                   "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/microsoft.onmicrosoft.com/oauth2/token"})

# COMMAND ----------

dbutils.fs.ls("/mnt/testblob")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 2 : Hadoop filesystem extension with access key (Set key in Spark Config)
# MAGIC
# MAGIC You can directly access with an extension, such as "abfss://" (when using Azure Data Lake Storage Gen 2) or "wasbs://" (when using other Azure blob) as follows.

# COMMAND ----------

# MAGIC %md
# MAGIC Before starting,
# MAGIC 1. Write following text in Spark Config on your cluster settings. (You can also set with code blocks, instead of using cluster's settings.)
# MAGIC     ```fs.azure.account.key.YOUR_STORAGE_ACCOUNT_NAME.dfs.core.windows.net YOUR_STORAGE_ACCOUNT_KEY```    
# MAGIC     For instance,
# MAGIC     ```fs.azure.account.key.demostore01.dfs.core.windows.net Fbur3ioIw+...```    
# MAGIC     **Also please configure on cluster with Databricks Runtime ML** (not only non-ML runtime) for "Exercise 05 : MLeap".
# MAGIC 2. Restart your cluster 

# COMMAND ----------

df = sqlContext.read.format("csv").\
  option("header", "true").\
  option("nullValue", "NA").\
  option("inferSchema", True).\
  load("abfss://container01@demostore01.dfs.core.windows.net/flight_weather.csv")
df.cache()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 3 : Use Azure Key Vault and Databricks Scope for Secure Access
# MAGIC
# MAGIC By interacting with Azure Key Vault, you can access your storage without writing secure information in Databricks.
# MAGIC
# MAGIC Before starting,
# MAGIC 1. Create your key vault resource in Azure Portal
# MAGIC 2. Create new secret and set service principal's secret as value in your key vault. Here we assume the key name is "testsecret01".
# MAGIC 2. Go to https://YOUR_AZURE_DATABRICKS_URL#secrets/createScope    
# MAGIC (Once you've created scope, you should manage with Databricks CLI.)    
# MAGIC ![Secret Scope](https://tsmatz.github.io/images/github/databricks/20191225_Create_Scope.jpg)
# MAGIC     - Set scope name. Here we assume the name is "scope01", which is needed for the following "dbutils" commands.
# MAGIC     - Select "Creator" (needing Azure Databricks Premium tier)
# MAGIC     - Input your key vault's settings, such as DNS name and resource id. (You can copy settings in key vault's "Properties".)

# COMMAND ----------

# MAGIC %md
# MAGIC Please set (change) the following values
# MAGIC
# MAGIC fs.azure.account.oauth2.client.id : ```SERVICE_PRINCIPAL_CLIENT_ID```    
# MAGIC fs.azure.account.oauth2.client.endpoint : ```https://login.microsoftonline.com/YOUR_DIRECTORY_DOMAIN/oauth2/token```    
# MAGIC source : ```abfss://YOUR_CONTAINER_NAME@YOUR_STORAGE_ACCOUNT_NAME.dfs.core.windows.net/```    

# COMMAND ----------

sp_secret = dbutils.secrets.get(scope = "scope01", key = "testsecret01")
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "31d00ab6-b48d-46eb-a540-dc6ce8cfbcd8",
           "fs.azure.account.oauth2.client.secret": sp_secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/microsoft.onmicrosoft.com/oauth2/token"}
dbutils.fs.mount(
  source = "abfss://container01@demostore01.dfs.core.windows.net",
  mount_point = "/mnt/testblob02",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/testblob02")

# COMMAND ----------

dbutils.fs.unmount("/mnt/testblob02")