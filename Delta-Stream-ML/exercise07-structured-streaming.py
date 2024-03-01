# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 07 : Structured Streaming (Basic)
# MAGIC
# MAGIC In all previous exercises, we run trainig in Databricks as batch processes. However you can also scale streaming inference with massive Databricks cluster as follows with Spark Structured Streaming.    
# MAGIC Structed Streaming enables you to manipulate each incoming streams as table rows. As a result, you can handle streaming data same as a standard Spark dataframe with a consistent programming manners. You can even join 2 streaming dataframes (stream-stream joins) such as ordinary join.
# MAGIC
# MAGIC First in this exercise, we learn the basics of Structured Streaming using primitive file's watches and memory sinks.    
# MAGIC When you've learned the idea of Structured Streaming, please proceed to next exercise !
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC First, generate json file (file01.json) for preparation. (See "Exercise 01" about DBFS.)

# COMMAND ----------

dbutils.fs.rm("/tmp/structured-streaming/events", recurse=True)
dbutils.fs.put(
  "/tmp/structured-streaming/events/file01.json",
  """{"event_name":"Open","event_time":1540601000}
{"event_name":"Open","event_time":1540601010}
{"event_name":"Fail","event_time":1540601020}
{"event_name":"Open","event_time":1540601030}
{"event_name":"Open","event_time":1540601040}
{"event_name":"Open","event_time":1540601050}
{"event_name":"Open","event_time":1540601060}
{"event_name":"Fail","event_time":1540601070}
{"event_name":"Open","event_time":1540601080}
{"event_name":"Open","event_time":1540601090}
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC See if your file is generated.

# COMMAND ----------

# MAGIC %fs head /tmp/structured-streaming/events/file01.json

# COMMAND ----------

# MAGIC %md
# MAGIC Read json as streaming dataframe using ```readStream()``` as follows.    
# MAGIC (In previous exercises, you used ```read()```, but here we use ```readStream()``` instead.)

# COMMAND ----------

# Create streaming dataframe
from pyspark.sql.types import *
from pyspark.sql.functions import *

read_schema = StructType([
  StructField("event_name", StringType(), True),
  StructField("event_time", TimestampType(), True)])
read_df = (
  spark
    .readStream                       
    .schema(read_schema)
    .option("maxFilesPerTrigger", 1)  # one file at a time
    .json("/tmp/structured-streaming/events/")
)

# COMMAND ----------

# MAGIC %md (The following is for debugging purpose and please skip. This uses normal ```read()``` function instead.)

# COMMAND ----------

# from pyspark.sql.types import *
# from pyspark.sql.functions import *

# test_schema = StructType([
#  StructField("event_name", StringType(), True),
#  StructField("event_time", TimestampType(), True)])
# test_df = (
#  spark
#    .read
#    .schema(test_schema)
#    .json("/tmp/structured-streaming/events/")
# )
# display(test_df)

# COMMAND ----------

# MAGIC %md
# MAGIC We start streaming processing by sinking into the memory. This memory data is querable by SQL as streaming query named "read_simple".    
# MAGIC By ```start()``` function, the streaming is kicked off and continue to run as background jobs.
# MAGIC
# MAGIC Note that you should not use memory sink in production, and this is only for debugging purpose.    
# MAGIC In the production system, the processed streaming data will be sinked into data store (such as, file, database, or data warehouse, etc) or some kind of streaming framework (such as, Apache Kafka, Azure Event Hub, or Amazon Kinesis, etc).    
# MAGIC (For the streaming architecture, we'll discuss in the next exercise.)

# COMMAND ----------

# Write results in memory
query1 = (
  read_df
    .writeStream
    .format("memory")
    .queryName("read_simple")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Query sinked data (memory data) using SQL.

# COMMAND ----------

# MAGIC %sql select event_name, date_format(event_time, "MMM-dd HH:mm") from read_simple order by event_time

# COMMAND ----------

# MAGIC %md
# MAGIC Add next events (10 events) into the folder, and then run and see the previous query's result again. (See how streaming is displayed in the previous background job.)

# COMMAND ----------

# Add new file in front end and see above result again
dbutils.fs.put(
  "/tmp/structured-streaming/events/file02.json",
  """{"event_name":"Open","event_time":1540601100}
{"event_name":"Open","event_time":1540601110}
{"event_name":"Fail","event_time":1540601120}
{"event_name":"Open","event_time":1540601130}
{"event_name":"Open","event_time":1540601140}
{"event_name":"Open","event_time":1540601150}
{"event_name":"Open","event_time":1540601160}
{"event_name":"Fail","event_time":1540601170}
{"event_name":"Open","event_time":1540601180}
{"event_name":"Open","event_time":1540601190}
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Next we generate another streaming dataframe with windowing operation.

# COMMAND ----------

# Windowing analysis
group_df = (                 
  read_df
    .groupBy(
      read_df.event_name, 
      window(read_df.event_time, "1 minute"))
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Define another streaming sink with query name "read_counts". (```start()``` function also kicks off the streaming and continue to run as background jobs.)    
# MAGIC As you can see here, you can define multiple sinks (streaming flows).

# COMMAND ----------

query2 = (
  group_df
    .writeStream
    .format("memory")
    .queryName("read_counts")
    .outputMode("complete")
    .start()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Query windowed data in memory.

# COMMAND ----------

# MAGIC %sql select event_name, date_format(window.end, "MMM-dd HH:mm") as event_time, count from read_counts order by event_time, event_name

# COMMAND ----------

# MAGIC %md
# MAGIC Again, add next 10 events into the folder and see the previous query's result again. (See how streaming is displayed in the previous background job.)

# COMMAND ----------

# Add new file in front end and see above result again
dbutils.fs.put(
  "/tmp/structured-streaming/events/file03.json",
  """{"event_name":"Open","event_time":1540601200}
{"event_name":"Open","event_time":1540601210}
{"event_name":"Fail","event_time":1540601220}
{"event_name":"Open","event_time":1540601230}
{"event_name":"Open","event_time":1540601240}
{"event_name":"Open","event_time":1540601250}
{"event_name":"Open","event_time":1540601260}
{"event_name":"Fail","event_time":1540601270}
{"event_name":"Open","event_time":1540601280}
{"event_name":"Open","event_time":1540601290}
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC After completed, cancel (stop) all previous streaming.

# COMMAND ----------

for s in spark.streams.active:
    s.stop()

# COMMAND ----------

