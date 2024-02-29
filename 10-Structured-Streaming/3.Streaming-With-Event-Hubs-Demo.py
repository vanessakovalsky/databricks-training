# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming with Azure EventHubs 
# MAGIC
# MAGIC ## Datasets Used
# MAGIC This notebook will consumn data being published through an EventHub with the following schema:
# MAGIC
# MAGIC - `Index`
# MAGIC - `Arrival_Time`
# MAGIC - `Creation_Time`
# MAGIC - `x`
# MAGIC - `y`
# MAGIC - `z`
# MAGIC - `User`
# MAGIC - `Model`
# MAGIC - `Device`
# MAGIC - `gt`
# MAGIC - `id`
# MAGIC - `geolocation`
# MAGIC
# MAGIC ## Library Requirements
# MAGIC
# MAGIC 1. the Maven library with coordinate `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18`
# MAGIC    - this allows Databricks `spark` session to communicate with an Event Hub
# MAGIC 2. the Python library `azure-eventhub`
# MAGIC    - this is allows the Python kernel to stream content to an Event Hub
# MAGIC
# MAGIC The next cell walks you through installing the Maven library. A couple cells below that, we automatically install the Python library using `%pip install`.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Lab Setup
# MAGIC
# MAGIC If you are running in an Azure Databricks environment that is already pre-configured with the libraries you need, you can skip to the next cell. To use this notebook in your own Databricks environment, you will need to create libraries, using the [Create Library](https://docs.azuredatabricks.net/user-guide/libraries.html) interface in Azure Databricks. Follow the steps below to attach the `azure-eventhubs-spark` library to your cluster:
# MAGIC
# MAGIC 1. In the left-hand navigation menu of your Databricks workspace, select **Compute**, then select your cluster in the list. If it's not running, start it now.
# MAGIC
# MAGIC   ![Select cluster](https://databricksdemostore.blob.core.windows.net/images/10-de-learning-path/select-cluster.png)
# MAGIC
# MAGIC 2. Select the **Libraries** tab (1), then select **Install New** (2). In the Install Library dialog, select **Maven** under Library Source (3). Under Coordinates, paste **com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.18** (4), then select **Install**.
# MAGIC   
# MAGIC   ![Databricks new Maven library](https://databricksdemostore.blob.core.windows.net/images/10-de-learning-path/install-eventhubs-spark-library.png)
# MAGIC
# MAGIC 3. Wait until the library successfully installs before continuing.
# MAGIC
# MAGIC   ![Library installed](https://databricksdemostore.blob.core.windows.net/images/10-de-learning-path/eventhubs-spark-library-installed.png)
# MAGIC
# MAGIC Once complete, return to this notebook to continue with the lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC
# MAGIC Run the following two cells to install the `azure-eventhub` Python library and configure our "classroom."

# COMMAND ----------

# This library allows the Python kernel to stream content to an Event Hub:
%pip install azure-eventhub

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC The following cell sets up a local streaming file read that we'll be writing to Event Hubs.

# COMMAND ----------

# MAGIC %run ./Includes/Streaming-Demo-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC In order to reach Event Hubs, you will need to insert the connection string-primary key you acquired at the end of the Getting Started notebook in this module. You acquired this from the Azure Portal, and copied it into Notepad.exe or another text editor.
# MAGIC
# MAGIC > Read this article to learn [how to acquire the connection string for an Event Hub](https://docs.microsoft.com/azure/event-hubs/event-hubs-create) in your own Azure Subscription.

# COMMAND ----------

event_hub_connection_string = "REPLACE-WITH-YOUR-EVENT-HUBS-CONNECTION-STRING" # Paste your Event Hubs connection string in the quotes to the left

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Azure Event Hubs</h2>
# MAGIC
# MAGIC Microsoft Azure Event Hubs is a fully managed, real-time data ingestion service.
# MAGIC You can stream millions of events per second from any source to build dynamic data pipelines and immediately respond to business challenges.
# MAGIC It integrates seamlessly with a host of other Azure services.
# MAGIC
# MAGIC Event Hubs can be used in a variety of applications such as
# MAGIC * Anomaly detection (fraud/outliers)
# MAGIC * Application logging
# MAGIC * Analytics pipelines, such as clickstreams
# MAGIC * Archiving data
# MAGIC * Transaction processing
# MAGIC * User telemetry processing
# MAGIC * Device telemetry streaming
# MAGIC * <b>Live dashboarding</b>

# COMMAND ----------


event_hub_name = "databricks-demo-eventhub"
connection_string = event_hub_connection_string + ";EntityPath=" + event_hub_name

print("Consumer Connection String: {}".format(connection_string))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream to Event Hub to Produce Stream

# COMMAND ----------


# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
ehWriteConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
}

checkpointPath = userhome + "/event-hub/write-checkpoint"
dbutils.fs.rm(checkpointPath,True)

(activityStreamDF
  .writeStream
  .format("eventhubs")
  .options(**ehWriteConf)
  .option("checkpointLocation", checkpointPath)
  .start())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Event Hubs Configuration</h2>
# MAGIC
# MAGIC Assemble the following:
# MAGIC * A `startingEventPosition` as a JSON string
# MAGIC * An `EventHubsConf`
# MAGIC   * to include a string with connection credentials
# MAGIC   * to set a starting position for the stream read
# MAGIC   * to throttle Event Hubs' processing of the streams

# COMMAND ----------


import json

# Create the starting position Dictionary
startingEventPosition = {
  "offset": "-1",
  "seqNo": -1,            # not in use
  "enqueuedTime": None,   # not in use
  "isInclusive": True
}

eventHubsConf = {
  "eventhubs.connectionString" : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
  "eventhubs.startingPosition" : json.dumps(startingEventPosition),
  "setMaxEventsPerTrigger": 100
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### READ Stream using Event Hubs
# MAGIC
# MAGIC The `readStream` method is a <b>transformation</b> that outputs a DataFrame with specific schema specified by `.schema()`.

# COMMAND ----------


from pyspark.sql.functions import col

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

eventStreamDF = (spark.readStream
  .format("eventhubs")
  .options(**eventHubsConf)
  .load()
)

eventStreamDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Most of the fields in this response are metadata describing the state of the Event Hubs stream. We are specifically interested in the `body` field, which contains our JSON payload.
# MAGIC
# MAGIC Noting that it's encoded as binary, as we select it, we'll cast it to a string.

# COMMAND ----------

bodyDF = eventStreamDF.select(col("body").cast("STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Each line of the streaming data becomes a row in the DataFrame once an <b>action</b> such as `writeStream` is invoked.
# MAGIC
# MAGIC Notice that nothing happens until you engage an action, i.e. a `display()` or `writeStream`.

# COMMAND ----------

display(bodyDF, streamName= "bodyDF")

# COMMAND ----------

# MAGIC %md
# MAGIC While we can see our JSON data now that it's cast to string type, we can't directly manipulate it.
# MAGIC
# MAGIC Before proceeding, stop this stream. We'll continue building up transformations against this streaming DataFrame, and a new action will trigger an additional stream.

# COMMAND ----------

for s in spark.streams.active:
  if s.name == "bodyDF":
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## <img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Parse the JSON payload
# MAGIC
# MAGIC The Event Hub acts as a sort of "firehose" (or asynchronous buffer) and displays raw data in the JSON format.
# MAGIC
# MAGIC If desired, we could save this as raw bytes or strings and parse these records further downstream in our processing.
# MAGIC
# MAGIC Here, we'll directly parse our data so we can interact with the fields.
# MAGIC
# MAGIC The first step is to define the schema for the JSON payload.
# MAGIC
# MAGIC > Both time fields are encoded as `LongType` here because of non-standard formatting.

# COMMAND ----------


from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType

schema = StructType([
  StructField("Arrival_Time", LongType(), True),
  StructField("Creation_Time", LongType(), True),
  StructField("Device", StringType(), True),
  StructField("Index", LongType(), True),
  StructField("Model", StringType(), True),
  StructField("User", StringType(), True),
  StructField("gt", StringType(), True),
  StructField("x", DoubleType(), True),
  StructField("y", DoubleType(), True),
  StructField("z", DoubleType(), True),
  StructField("geolocation", StructType([
    StructField("PostalCode", StringType(), True),
    StructField("StateProvince", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True)
  ]), True),
  StructField("id", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Parse the data
# MAGIC
# MAGIC Next we can use the function `from_json` to parse out the full message with the schema specified above.
# MAGIC
# MAGIC When parsing a value from JSON, we end up with a single column containing a complex object.

# COMMAND ----------


from pyspark.sql.functions import col, from_json

parsedEventsDF = bodyDF.select(
  from_json(col("body"), schema).alias("json"))

parsedEventsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we can further parse this to flatten the schema entirely and properly cast our time fields.

# COMMAND ----------


from pyspark.sql.functions import from_unixtime

flatSchemaDF = (parsedEventsDF
  .select(from_unixtime(col("json.Arrival_Time")/1000).alias("Arrival_Time").cast("timestamp"),
          (col("json.Creation_Time")/1E9).alias("Creation_Time").cast("timestamp"),
          col("json.Device").alias("Device"),
          col("json.Index").alias("Index"),
          col("json.Model").alias("Model"),
          col("json.User").alias("User"),
          col("json.gt").alias("gt"),
          col("json.x").alias("x"),
          col("json.y").alias("y"),
          col("json.z").alias("z"),
          col("json.id").alias("id"),
          col("json.geolocation.country").alias("country"),
          col("json.geolocation.city").alias("city"),
          col("json.geolocation.PostalCode").alias("PostalCode"),
          col("json.geolocation.StateProvince").alias("StateProvince"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC This flat schema provides us the ability to view each nested field as a column.

# COMMAND ----------

display(flatSchemaDF)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Stop all active streams

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Event Hubs FAQ
# MAGIC
# MAGIC This [FAQ](https://github.com/Azure/azure-event-hubs-spark/blob/master/FAQ.md) can be an invaluable reference for occasional Spark-EventHub debugging.
# MAGIC