# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 11 : Orchestration with Azure Data Services
# MAGIC
# MAGIC Finally we create ETL (Extract, Transform, and Load) batch flows with Azure Databricks for production.    
# MAGIC There are following 3 options for orchestration :
# MAGIC
# MAGIC - Generate and schedule jobs in Databricks, or invoke notebook manually (on-demand) from outside of Databricks (through REST API, etc).    
# MAGIC (You can also integrate multiple notebooks as workflow each other with input parameters and output results.)
# MAGIC - Integrate using Azure Data Factory (ADF). Also invoked by scheduling trigger or manual execution (through REST API, SDK, etc).
# MAGIC - Involve in ML pipeline on [Azure Machine Learning services](https://tsmatz.wordpress.com/2018/11/20/azure-machine-learning-services/).
# MAGIC
# MAGIC Here we create orchestration flow using ADF, in which we extract (ingest) data from public web site, transform on Databricks with multiple workers, and load into Azure Synapse Analytics. (Then you can see the results using Power BI, etc.)
# MAGIC
# MAGIC ![Pipeline Diagram](https://tsmatz.github.io/images/github/databricks/20191114_Pipeline_Diagram.jpg)
# MAGIC
# MAGIC > Note : You can also build data transformation on Azure Databricks with visual UI using new Azure Data Factory Data Flows (Preview).
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare resource for Azure Data Factory (ADF)
# MAGIC
# MAGIC 1. Go to [Azure Portal](https://portal.azure.com) and create a new "Data Factory" resource. (Click "Create a resource" and select "Analytics" - "Data Factory" in the left navigation.)
# MAGIC     - Select "V2" for Data Factory version in creation wizard.
# MAGIC 2. After a ADF resource is generated, go to resource blade and click "Author & Monitor". (See below.)    
# MAGIC Then ADF user interface (UI) application starts.    
# MAGIC ![ADF Blade](https://tsmatz.github.io/images/github/databricks/20190207_ADF_Blade.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Flight Delay Dataset
# MAGIC
# MAGIC Here we insert a pipeline activity which downloads flight delay dataset of year 2008 (originated by [BTS (Bureau of Transportation Statistics in US)](https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time), approximately 700 MB, 7,000,000 rows) from public web site.
# MAGIC
# MAGIC 1. In ADF GUI, start to create pipeline.    
# MAGIC ![Create Pipeline](https://tsmatz.github.io/images/github/databricks/20190207_Create_Pipeline.jpg)
# MAGIC 2. In pipeline GUI editor, create new connection. (Click "Connections" and "New" as following picture.)    
# MAGIC ![Create Connection](https://tsmatz.github.io/images/github/databricks/20190207_Create_Connections.jpg)    
# MAGIC In the next wizard, select "HTTP" for data store.    
# MAGIC ![Select HTTP](https://tsmatz.github.io/images/github/databricks/20190207_Datastore_HTTP.jpg)    
# MAGIC In the next wizard, set following properties and finish.
# MAGIC     - Base URL : http://stat-computing.org
# MAGIC     - Authentication Type : Anonymous
# MAGIC 3. In pipeline GUI editor, create new connection again and select "Azure Data Lake Storage Gen2" for data store.    
# MAGIC ![Select ADLS Gen2](https://tsmatz.github.io/images/github/databricks/20190207_Datastore_ADLSGen2.jpg)    
# MAGIC In the next wizard, select "Managed Service Identity (MSI)" in "Authentication method" property and copy generated service identity application ID into the clipboard. (See below.)    
# MAGIC ![Authentication Setting for ADLS Gen2](https://tsmatz.github.io/images/github/databricks/20190207_ADF_ADLSAuth.jpg)
# MAGIC 4. Go to Azure Data Lake Storage Gen 2 resource in Azure Portal and click "Access Control (IAM)" on navigation.
# MAGIC     - Click "Add role assignment"
# MAGIC     - Set the following and save
# MAGIC         - Role : Storage Blob Data Contributor
# MAGIC         - Assign : (previously copied service principal)
# MAGIC 5. In pipeline GUI editor, drag and insert "Copy Data" activity in your pipeline.    
# MAGIC ![Insert Copy Activity](https://tsmatz.github.io/images/github/databricks/20190207_Copy_Activity.jpg)
# MAGIC 6. Select "Source" tab and click "New". (See below.)    
# MAGIC ![Soutce Setting](https://tsmatz.github.io/images/github/databricks/20190207_New_Datasource.jpg)
# MAGIC     - In the wizard, select "HTTP" for data source type
# MAGIC     - In dataset setting, select "Connection" tab and fill the following properties.
# MAGIC         - Linked service : (select previously created "HTTP" connection)
# MAGIC         - Relative Url : /dataexpo/2009/2008.csv.bz2
# MAGIC         - Request Method : GET
# MAGIC         - Compression type : BZip2
# MAGIC         - Compression level : Optimal
# MAGIC         - Binary Copy : Yes    
# MAGIC         ![Dataset Setting](https://tsmatz.github.io/images/github/databricks/20190207_ADF_FlightData.jpg)
# MAGIC 7. Select "Sink" tab in the activity setting and create new destination dataset (click "New").    
# MAGIC ![Sink Setting](https://tsmatz.github.io/images/github/databricks/20190207_New_Datasink.jpg)
# MAGIC     - In the wizard, select "Azure Data Lake Storage Gen2" for data source type
# MAGIC     - In dataset setting, select "Connection" tab and fill the following properties
# MAGIC         - Linked service : (select previously created "Azure Data Lake Storage Gen2" connection)
# MAGIC         - File Path : (container name in your ADLS Gen2)
# MAGIC         - Binary Copy : Yes
# MAGIC 8. Publish entire pipeline. (If you're linking your own repository, json file will be saved as pipeline definitions in your repository.)
# MAGIC 9. You can easily see whether your settings are successfully configured by debugging. (Click "Debug" button.)    
# MAGIC ![ADF Debug](https://tsmatz.github.io/images/github/databricks/20190207_ADF_Debug.jpg)
# MAGIC
# MAGIC > Note : In production use, you can pass the parameters (such as file location, etc) into your pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare a resource for Azure Databricks
# MAGIC
# MAGIC Go to cluster settings on Azure Databricks and set storage access key for Azure Data Lake Storage (Gen2) in Spark config on your cluster.    
# MAGIC Follow "[Exercise 01 : Storage Settings](https://tsmatz.github.io/azure-databricks-exercise/exercise01-blob.html)" for detailed steps.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Dataset on Azure Databricks
# MAGIC
# MAGIC Here we insert Databricks' notebook activity and run notebook against downloaded csv.    
# MAGIC Using Azure Data Lake Storage as common data store, the data is not transferred across each activities.
# MAGIC
# MAGIC 1. Launch Azure Databricks portal and go to workspace.    
# MAGIC Click user profile icon (see below on the right top corner) and open user settings UI.    
# MAGIC Generate access token by clicking "Generate New Token" button    
# MAGIC ![Generate Databricks Token](https://tsmatz.github.io/images/github/databricks/20190207_Databricks_Token.jpg)
# MAGIC 2. Go to pipeline GUI editor on ADF UI and create new connection. (Click "Connections" and "New" button.)    
# MAGIC In the wizard, click "Compute" tab and select "Azure Databricks" from the list of connection type.    
# MAGIC ![Select Azure Databricks](https://tsmatz.github.io/images/github/databricks/20190207_Datastore_Databricks.jpg)    
# MAGIC In the connection setting, fill the following properties and finish. (Here we use interactive cluster for debugging, but it's better to use job cluster for production use.)
# MAGIC     - Databricks workspace : (select your Databricks service)
# MAGIC     - Select cluster : Existing interactive cluster
# MAGIC     - Access token : (previously generated access token for Azure Databricks)
# MAGIC     - Choose from existing cluster : (select your interactive cluster)    
# MAGIC     ![Databricks Setting](https://tsmatz.github.io/images/github/databricks/20190207_ADF_Databricks.jpg)
# MAGIC 3. Drag notebook activity into your pipeline and connect with the previous "Copy Data" activity.    
# MAGIC ![Insert Notebook Activity](https://tsmatz.github.io/images/github/databricks/20190207_Notebook_Activity.jpg)
# MAGIC     - Click "Azure Databricks" tab in activity setting and select above connection as Databricks linked service.    
# MAGIC       Click "Edit" and set properties again.
# MAGIC     - Next select "Settings" tab. Click "Browse" button in "notebook path" section and select Exercise11/simple-transform-notebook in this exercise.    
# MAGIC     ![Databricks Dataset Setting](https://tsmatz.github.io/images/github/databricks/20190207_Databricks_Dataset.jpg)
# MAGIC 4. Publish entire pipeline again.    
# MAGIC Please press "Debug" button and see if the parquet (/flights_parquet) is generated.    
# MAGIC (The cluster is automatically turned on and terminated when it's inactive for a while.)
# MAGIC
# MAGIC > Note : You can also integrate using Data Lake Analytics U-SQL activity instead of Databricks notebook activity in the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare resource for Azure Synapse Analytics (formerly, SQL DW)
# MAGIC
# MAGIC 1. Create a new "Azure Synapse Analytics" (formerly, Azure SQL Data Warehouse) resource in [Azure Portal](https://portal.azure.com). (Click "Create a resource" and select "Database" - "Azure Synapse Analytics" in the left navigation.)    
# MAGIC ![New SQL DW](https://tsmatz.github.io/images/github/databricks/20190207_New_Sqldw.jpg)
# MAGIC 2. Go to firewall settings in your database server and open ports in order to access from your desktop client.
# MAGIC 3. Connect database using database tools (IDEs), such as Azure Data Studio, Visual Studio, and so forth.    
# MAGIC   And run the following query.
# MAGIC
# MAGIC ```
# MAGIC CREATE TABLE flightdat_dim(
# MAGIC [Year] [int] NULL,
# MAGIC [Month] [int] NULL,
# MAGIC [DayofMonth] [int] NULL,
# MAGIC [DayOfWeek] [int] NULL,
# MAGIC [UniqueCarrier] [varchar](50) NULL,
# MAGIC [Origin] [varchar](50) NULL,
# MAGIC [Dest] [varchar](50) NULL,
# MAGIC [CRSDepTime] [int] NULL,
# MAGIC [CRSArrTime] [int] NULL,
# MAGIC [DepDelay] [int] NULL,
# MAGIC [ArrDelay] [int] NULL,
# MAGIC [CancelledFlag] [bit] NULL,
# MAGIC [DivertedFlag] [bit] NULL,
# MAGIC [ArrDelay15Flag] [bit] NULL);
# MAGIC
# MAGIC CREATE STATISTICS flightdat_stat on flightdat_dim(Year, Month, DayofMonth);
# MAGIC GO
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data into Azure Synapse Analytics
# MAGIC Once data (parquet) is generated, you can efficiently load (import) data into Azure Synapse Analytics with PolyBase by using any of 3 ways :
# MAGIC
# MAGIC - Using T-SQL directly
# MAGIC - Using Azure Data Factory connector for Azure Synapse Analytics
# MAGIC - Using Azure Databricks connector for Azure Synapse Analytics
# MAGIC
# MAGIC Here we use ADF connector, but later I show you T-SQL scripts for your reference.
# MAGIC
# MAGIC 4. Go to pipeline GUI editor on ADF UI and create new connection. (Click "Connections" and "New" button.)    
# MAGIC In the wizard, select "Azure Synapse Analytics" (formerly, Azure SQL Data Warehouse) from the list of connections.    
# MAGIC ![Select Azure SQL Data Warehouse](https://tsmatz.github.io/images/github/databricks/20190207_Datastore_SQLDW.jpg)    
# MAGIC In connection setting, fill the following properties.
# MAGIC     - server name : (your db server name)
# MAGIC     - database name : (your database name)
# MAGIC     - Authentication type : SQL Authentication
# MAGIC     - User name : (server user name)
# MAGIC     - Password : (password)    
# MAGIC     ![SQL DW Connection Setting](https://tsmatz.github.io/images/github/databricks/20190207_ADF_SQLDW.jpg)
# MAGIC 5. Drag "Copy Data" activity into the pipeline and connect with notebook activity. (See below.)    
# MAGIC ![Copy Activity](https://tsmatz.github.io/images/github/databricks/20190207_Copy_Activity2.jpg)
# MAGIC 6. In the activity setting, click "Source" tab and create new source dataset (click "New")
# MAGIC     - In the wizard, select "Azure Data Lake Storage Gen2" from the list of dataset type
# MAGIC     - In dataset setting, select "Connection" tab and fill as follows
# MAGIC         - Linked service : (previously generated ADLS Gen2 connection)
# MAGIC         - File Path - Directory : (your container name/flights_parquet)
# MAGIC         - File Path - File : part-*
# MAGIC         - File Format : Parquet format    
# MAGIC         ![Source Setting](https://tsmatz.github.io/images/github/databricks/20190207_Parquet_Source.jpg)
# MAGIC 7. In the activity setting, click "Sink" tab and create new destination dataset (click "New")
# MAGIC     - In the wizard, select "Azure Synapse Analytics" from the list of dataset type
# MAGIC     - In dataset setting, click "Edit", select "Connection" tab, and fill as follows
# MAGIC         - Linked service : (previously generated Synapse Analytics connection)
# MAGIC         - Table : [dbo].[flightdat_dim]
# MAGIC 8. **Warning : Currently (Feb 2019) ADF doesn't support ADLS Gen2 storage (with hierarchical namespace) for Synapse Analytics ingestion by PolyBase. For this reason, you need interim staging account.**    
# MAGIC Please proceed the following extra steps.
# MAGIC     - Create another storage account (not Gen2) resource in Azure Portal.
# MAGIC     - Create a new connection for "Azure Blob Storage" in ADF pipeline GUI editor.
# MAGIC     - In this activity setting ("Copy Data" activity for Synapse Analytics), click "Settings" tab, turn on "Enable staging", and select generated blob connection for staging account.
# MAGIC
# MAGIC > Note : The complex data type (nested type) is not supported for both Azure Synapse Analytics and ADF. Then you must convert to flat format with computing activity (Azure Databricks, Azure Data Lake Analytics, etc).    
# MAGIC > Here we simply uses flat data format.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Entire Pipeline
# MAGIC
# MAGIC In pipeline GUI, click "Debug" and run entire pipeline.
# MAGIC
# MAGIC ![Debug and Run](https://tsmatz.github.io/images/github/databricks/20190207_Run_Result.jpg)
# MAGIC
# MAGIC After the pipeline succeeded, see the data in Azure Synapse Analytics with following query using database tools (Azure Data Studio, and so forth).
# MAGIC
# MAGIC ```select * from flightdat_dim where Year = 2008 and Month = 5 and DayOfMonth = 25```
# MAGIC
# MAGIC ![Query Result](https://tsmatz.github.io/images/github/databricks/20190207_SQL_Result.jpg)
# MAGIC
# MAGIC When your experimentation is completed, you can stop (pause) Azure Synapse Analytics for saving money ! (Click "Pause" in Synapse Analytics dashboard in Azure Portal.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference : Load with T-SQL
# MAGIC
# MAGIC Here I show you how to load parquet into Azure Synapse Analytics with **T-SQL**. (Not needed interim staging account)
# MAGIC
# MAGIC ```
# MAGIC CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'P@ssw0rd';
# MAGIC GO
# MAGIC
# MAGIC CREATE DATABASE SCOPED CREDENTIAL flightdat_cred
# MAGIC WITH IDENTITY = 'user',
# MAGIC SECRET = '{ADLS Gen2 access key}';
# MAGIC Go
# MAGIC
# MAGIC CREATE EXTERNAL DATA SOURCE flightdat_src
# MAGIC WITH
# MAGIC (
# MAGIC   TYPE = HADOOP, 
# MAGIC   LOCATION = N'abfss://{container name}@{storage account name}.dfs.core.windows.net/', 
# MAGIC   CREDENTIAL = flightdat_cred
# MAGIC );
# MAGIC GO
# MAGIC
# MAGIC CREATE EXTERNAL FILE FORMAT parquet_format
# MAGIC WITH (FORMAT_TYPE = PARQUET);
# MAGIC GO
# MAGIC
# MAGIC CREATE EXTERNAL TABLE flightdat_import(
# MAGIC [Year] [int] NULL,
# MAGIC [Month] [int] NULL,
# MAGIC [DayofMonth] [int] NULL,
# MAGIC [DayOfWeek] [int] NULL,
# MAGIC [UniqueCarrier] [varchar](50) NULL,
# MAGIC [Origin] [varchar](50) NULL,
# MAGIC [Dest] [varchar](50) NULL,
# MAGIC [CRSDepTime] [int] NULL,
# MAGIC [CRSArrTime] [int] NULL,
# MAGIC [DepDelay] [int] NULL,
# MAGIC [ArrDelay] [int] NULL,
# MAGIC [CancelledFlag] [bit] NULL,
# MAGIC [DivertedFlag] [bit] NULL,
# MAGIC [ArrDelay15Flag] [bit] NULL)
# MAGIC WITH
# MAGIC (
# MAGIC   DATA_SOURCE = flightdat_src,
# MAGIC   FILE_FORMAT = parquet_format,
# MAGIC   LOCATION = '/flights_parquet'
# MAGIC );
# MAGIC GO
# MAGIC
# MAGIC CREATE TABLE flightdat_dim
# MAGIC WITH
# MAGIC (
# MAGIC   DISTRIBUTION = ROUND_ROBIN,
# MAGIC   CLUSTERED INDEX (Year, Month, DayofMonth)
# MAGIC )
# MAGIC AS SELECT * FROM flightdat_import;
# MAGIC GO
# MAGIC
# MAGIC CREATE STATISTICS flightdat_stat on flightdat_dim(Year, Month, DayofMonth);
# MAGIC GO```
# MAGIC
# MAGIC
# MAGIC > Note : With new Azure Synapse Analytics, you can also use ```COPY``` statement for loading data without creating external table and credentials settings.