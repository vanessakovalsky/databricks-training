# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Process real-time tweets with Structured Streaming
# MAGIC
# MAGIC [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. You can express your streaming computation the same way you would express a batch computation on static data. The Spark SQL engine will take care of running it incrementally and continuously and updating the final result as streaming data continues to arrive. You can use the Dataset/DataFrame API in Scala, Java, Python or R to express streaming aggregations, event-time windows, stream-to-batch joins, etc. The computation is executed on the same optimized Spark SQL engine. Finally, the system ensures end-to-end exactly-once fault-tolerance guarantees through checkpointing and Write Ahead Logs. In short, Structured Streaming provides fast, scalable, fault-tolerant, end-to-end exactly-once stream processing without the user having to reason about streaming.
# MAGIC
# MAGIC ### Programming Model
# MAGIC The key idea in Structured Streaming is to treat a live data stream as a table that is being continuously appended. This leads to a new stream processing model that is very similar to a batch processing model. You will express your streaming computation as standard batch-like query as on a static table, and Spark runs it as an incremental query on the unbounded input table. Let’s understand this model in more detail.
# MAGIC
# MAGIC ### Basic Concepts
# MAGIC Consider the input data stream as the “Input Table”. Every data item that is arriving on the stream is like a new row being appended to the Input Table.
# MAGIC
# MAGIC ![](https://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png)
# MAGIC
# MAGIC A query on the input will generate the “Result Table”. Every trigger interval (say, every 1 second), new rows get appended to the Input Table, which eventually updates the Result Table. Whenever the result table gets updated, we would want to write the changed result rows to an external sink.
# MAGIC
# MAGIC ![](https://spark.apache.org/docs/latest/img/structured-streaming-model.png)
# MAGIC
# MAGIC The “Output” is defined as what gets written out to the external storage. The output can be defined in a different mode:
# MAGIC
# MAGIC * Complete Mode - The entire updated Result Table will be written to the external storage. It is up to the storage connector to decide how to handle writing of the entire table.
# MAGIC
# MAGIC * Append Mode - Only the new rows appended in the Result Table since the last trigger will be written to the external storage. This is applicable only on the queries where existing rows in the Result Table are not expected to change.
# MAGIC
# MAGIC * Update Mode - Only the rows that were updated in the Result Table since the last trigger will be written to the external storage (available since Spark 2.1.1). Note that this is different from the Complete Mode in that this mode only outputs the rows that have changed since the last trigger. If the query doesn’t contain aggregations, it will be equivalent to Append mode.

# COMMAND ----------

# connecting to Event Hubs and creating the stream
# Azure Event Hubs access credentials
event_hubs_namespace = '(enter)'
event_hubs_name = '(enter)'
event_hubs_key_name = '(enter)'
event_hubs_key_value = '(enter)'

connectionString = "Endpoint=sb://"+event_hubs_namespace+".servicebus.windows.net/;SharedAccessKeyName="+event_hubs_key_name+";SharedAccessKey="+event_hubs_key_value+";EntityPath="+event_hubs_name
ehConf = {
  'eventhubs.connectionString' : connectionString
}

incomingStream = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

# COMMAND ----------

# since the message payload is JSON, let's convert that into individual columns.

from pyspark.sql.functions import from_json
from pyspark.sql.types import *

schema = StructType([
  StructField("created_at", StringType()), 
  StructField("timestamp", TimestampType()), 
  StructField("text", StringType())
])

tweets = incomingStream \
  .withColumn("body", incomingStream["body"].cast("string")) \
  .select(from_json("body", schema).alias("c")) \
  .select("c.*")

# COMMAND ----------

display(tweets)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Sentiment Analysis on tweets
# MAGIC
# MAGIC We're going to use the **TextBlob** library to analyze the tweets.
# MAGIC
# MAGIC TextBlob is a Python (2 and 3) library for processing textual data. It provides a simple API for diving into common natural language processing (NLP) tasks such as part-of-speech tagging, noun phrase extraction, sentiment analysis, classification, translation, and more.
# MAGIC
# MAGIC We add a new column to the dataframe, called *sentiment*, which will contain a value between -1 (negative) to 1 (positive).

# COMMAND ----------

from pyspark.sql.functions import udf
from textblob import *

def getSentiment(text):
  sentiment = TextBlob(text)
  return sentiment.sentiment.polarity

getSentimentUdf = udf(lambda text: getSentiment(text))

tweetsWithSentiment = tweets \
  .withColumn("sentiment", getSentimentUdf("text"))

# COMMAND ----------

display(tweetsWithSentiment)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Operations on streaming DataFrames/Datasets
# MAGIC You can apply all kinds of operations on streaming DataFrames/Datasets – ranging from untyped, SQL-like operations (e.g. select, where, groupBy), to typed RDD-like operations (e.g. map, filter, flatMap). See the SQL programming guide for more details. Let’s take a look at a few example operations that you can use.

# COMMAND ----------

display(tweetsWithSentiment.where("sentiment > 0.1"))


# COMMAND ----------

# You can also register a streaming DataFrame/Dataset as a temporary view and then apply SQL commands on it.

tweetsWithSentiment.createOrReplaceTempView("tweetsWithSentiment")
display(spark.sql("select count(*) from tweetsWithSentiment"))

# COMMAND ----------

#Note, you can identify whether a DataFrame/Dataset has streaming data or not by using df.isStreaming.

print(tweetsWithSentiment.isStreaming)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Window Operations on Event Time
# MAGIC Aggregations over a sliding event-time window are straightforward with Structured Streaming and are very similar to grouped aggregations. In a grouped aggregation, aggregate values (e.g. counts) are maintained for each unique value in the user-specified grouping column. In case of window-based aggregations, aggregate values are maintained for each window the event-time of a row falls into. Let’s understand this with an illustration.
# MAGIC
# MAGIC ![](https://spark.apache.org/docs/latest/img/structured-streaming-window.png)
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import window

# Group the data by time window and compute the count of each group
windowedCounts = tweetsWithSentiment.groupBy(
    window("timestamp", "1 minute")
).agg({"text": "count", "sentiment": "avg"})

display(windowedCounts)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The SQL version of the above query is below. You need to have created the `tweetsWithSentiment` temporary view as described earlier. 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT WINDOW(timestamp, "1 minute"), COUNT(text) AS count, AVG(sentiment) as avgsentiment
# MAGIC FROM tweetsWithSentiment
# MAGIC GROUP BY WINDOW(timestamp, "1 minute")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC For more information about the various Structured Streaming features, head over to the [documentation page](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Exercises
# MAGIC
# MAGIC 1. The example `GROUP BY WINDOW` above creates *tumbling* (non-overlapping) time windows. Can you modify the query so you get two-minute long windows, starting every minute (*sliding windows*)?