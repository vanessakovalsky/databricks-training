# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 09 : Delta Lake (Databricks Delta)
# MAGIC
# MAGIC Delta format is built on parquet format with transaction tracking (journals). As you can see in this exercise, it brings you both reliability and performance by a consistent spark read/write manner.    
# MAGIC With this controlled format, it realizes :
# MAGIC
# MAGIC - Provides isolation level (ACID transaction) which avoid conflicts
# MAGIC - Enable snapshots for data with time travel or versioning
# MAGIC - Skip data (files), which is not referred by query, using Z-Ordering
# MAGIC - Optimize the layout of data (such as, streamed and ingested data) including Z-Ordering for querying whole set of data (Small file compactions)
# MAGIC - You can soon remove data (vacuum), which is no longer needed
# MAGIC - Support MERGE command (e.g, Support efficient upserts)
# MAGIC - Prevent polluting tables with dirty data (Schema enforcement)
# MAGIC
# MAGIC This needs **Databricks Runtime 4.1 or above**.
# MAGIC
# MAGIC > Note : Delta table has some constraints compared with normal parquet format. See [here](https://docs.databricks.com/delta/delta-faq.html) for the supported operations.
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

dbutils.fs.rm("/tmp/structured-streaming/events", recurse=True)
dbutils.fs.put(
  "/tmp/structured-streaming/events/file_org.json",
  """{"event_id":0,"event_name":"Open","event_time":1540601000}
{"event_id":1,"event_name":"Open","event_time":1540601010}
{"event_id":2,"event_name":"Fail","event_time":1540601020}
{"event_id":3,"event_name":"Open","event_time":1540601030}
{"event_id":4,"event_name":"Open","event_time":1540601040}
{"event_id":5,"event_name":"Open","event_time":1540601050}
{"event_id":6,"event_name":"Open","event_time":1540601060}
{"event_id":7,"event_name":"Fail","event_time":1540601070}
{"event_id":8,"event_name":"Open","event_time":1540601080}
{"event_id":9,"event_name":"Open","event_time":1540601090}
""", True)

# COMMAND ----------

dbutils.fs.rm("/tmp/delta", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Create delta table (delta lake) as follows. All you have to do is to just say "delta" instead of "parquet".

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS testdb;
# MAGIC USE testdb;
# MAGIC DROP TABLE IF EXISTS testdb.table01;
# MAGIC CREATE TABLE testdb.table01(
# MAGIC   event_id INT,
# MAGIC   event_name STRING,
# MAGIC   event_time TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION "/tmp/delta/events"

# COMMAND ----------

# MAGIC %md
# MAGIC Write streaming data into delta table.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Write data without streaming into delta table
# read_schema = StructType([
#  StructField("event_id", IntegerType(), False),
#  StructField("event_name", StringType(), True),
#  StructField("event_time", TimestampType(), True)])
# df1 = spark.read.schema(read_schema).json("/tmp/structured-streaming/events")
# df1.write.mode("overwrite").format("delta").save("/tmp/delta/events")

# Streaming reads and append into delta table (Start !)
read_schema = StructType([
  StructField("event_id", IntegerType(), False),
  StructField("event_name", StringType(), True),
  StructField("event_time", TimestampType(), True)])
df2 = (spark.readStream
  .option("maxFilesPerTrigger", "1")
  .schema(read_schema)
  .json("/tmp/structured-streaming/events"))
(df2.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/tmp/delta/checkpoint")
  .option("path", "/tmp/delta/events").start())

# COMMAND ----------

# MAGIC %md
# MAGIC Insert additional 100 rows one by one. These rows will be fragmented in delta.

# COMMAND ----------

import random
for x in range(100):
  event_id = 10 + x
  rnd = random.randint(0,1)
  event_name="Open"
  if rnd == 1:
    event_name="Fail"
  rnd = random.randint(-30000000,30000000)
  event_time = 1540601000 + rnd
  dbutils.fs.put(
    "/tmp/structured-streaming/events/file%d.json" % (x),
    "{\"event_id\":%d,\"event_name\":\"%s\",\"event_time\":%d}" % (event_id, event_name, event_time),
    True)

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until all 110 records (rows) are generated ...

# COMMAND ----------

# MAGIC %sql select count(*) from testdb.table01

# COMMAND ----------

# MAGIC %md
# MAGIC See how long does it take.    
# MAGIC (Query is evaluated in first time, then please run multiple times.)

# COMMAND ----------

# MAGIC %sql select event_name, event_time from testdb.table01 where event_name = "Open" order by event_time

# COMMAND ----------

# MAGIC %md
# MAGIC Run ```OPTIMIZE``` command.    
# MAGIC This command compacts a lot of small files into large ones to speed up, and improves performance for batch query by specifing ```ZORDER``` clustering. (When there exist massively large data, only required partitions are referred by query and other partitions will be skipped and ignored.)    
# MAGIC You can also set **automatic optimization (auto optimize)** specifying a property ```delta.autoOptimize = true``` in table.

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE testdb;
# MAGIC OPTIMIZE "/tmp/delta/events"
# MAGIC ZORDER BY (event_name, event_time)

# COMMAND ----------

# MAGIC %md
# MAGIC See how long does it take again, and compare with the previous result. You could find it's more performant. (Query is evaluated in first time, then please run multiple times.)    
# MAGIC The data is very small, then it might just be a little bit difference ...

# COMMAND ----------

# MAGIC %sql select event_name, event_time from testdb.table01 where event_name = "Open" order by event_time

# COMMAND ----------

# MAGIC %md
# MAGIC Now we try to corrupt our data and rollback with delta. Before starting, please check the record's number of "Open" events.

# COMMAND ----------

# MAGIC %sql SELECT count(*) from testdb.table01 where event_name = 'Open'

# COMMAND ----------

# MAGIC %md
# MAGIC Now we corrupt our data intentionally.

# COMMAND ----------

# MAGIC %sql UPDATE testdb.table01 SET event_name = 'Fail' WHERE event_name = 'Open'

# COMMAND ----------

# MAGIC %md
# MAGIC Now you can see these events are all lost.

# COMMAND ----------

# MAGIC %sql SELECT count(*) from testdb.table01 where event_name = 'Open'

# COMMAND ----------

# MAGIC %md
# MAGIC With delta table, the updated state and data are recorded in version history.    
# MAGIC Please pick up the version number of latest correct data.

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY testdb.table01

# COMMAND ----------

# MAGIC %md
# MAGIC Rollback data with the version number of correct data. (Please replace this number to your appropriate value.)    
# MAGIC You can also get history data using timestamp, instead of version number.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO testdb.table01 dst
# MAGIC USING testdb.table01 VERSION AS OF 102 src
# MAGIC ON src.event_id = dst.event_id
# MAGIC WHEN MATCHED THEN UPDATE SET *

# COMMAND ----------

# MAGIC %md
# MAGIC Check if data is recovered.

# COMMAND ----------

# MAGIC %sql SELECT count(*) from testdb.table01 where event_name = 'Open'

# COMMAND ----------

# MAGIC %md
# MAGIC With ```VACUUM```, you can remove files that are no longer in the latest state of the transaction log for the table. (i.e, it cleans up history.)    
# MAGIC Note that there exists a retention period (7 days by default), then it cannot be immediately removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM testdb.table01

# COMMAND ----------

# MAGIC %md
# MAGIC After completed, cancel (stop) previous streaming.

# COMMAND ----------

for s in spark.streams.active:
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove delta.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists testdb cascade

# COMMAND ----------

