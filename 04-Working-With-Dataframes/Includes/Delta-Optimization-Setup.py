# Databricks notebook source
from pyspark.sql.functions import expr, col, from_unixtime, to_date

dbutils.fs.rm(userhome + "/delta/iot-events/", True)

streamingEventPath = "/mnt/training/structured-streaming/events/"

(spark
  .read
  .option("inferSchema", "true")
  .json(streamingEventPath)
  .withColumn("date", to_date(from_unixtime(col("time").cast("Long"),"yyyy-MM-dd")))
  .withColumn("deviceId", expr("cast(rand(5) * 100 as int)"))
  .repartition(200)
  .write
  .mode("overwrite")
  .format("delta")
  .partitionBy("date")
  .save(userhome + "/delta/iot-events/"))

# COMMAND ----------

