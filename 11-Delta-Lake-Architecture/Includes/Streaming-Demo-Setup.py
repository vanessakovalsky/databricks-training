# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, DoubleType, LongType

dbutils.fs.rm(userhome + "/streaming-demo", True)

activityDataDF = (spark.read
                  .json("/mnt/training/definitive-guide/data/activity-data-with-geo.json/"))

tempJson = spark.createDataFrame(activityDataDF.toJSON(), StringType()).withColumnRenamed("value", "body")

tempJson.write.mode("overwrite").format("delta").save(userhome + "/streaming-demo")

activityStreamDF = (spark.readStream
  .format("delta")
  .option("maxFilesPerTrigger", 1)
  .load(userhome + "/streaming-demo")
)