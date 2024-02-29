# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Analyzing real-time data with Spark Streaming
# MAGIC
# MAGIC **Spark Streaming** is a scalable, fault-tolerant streaming processing system. As part of Apache Spark, it integrates with MLlib, SQL, DataFrames, and GraphX. As for Spark 2.0, we have also released [Structured Streaming](https://databricks.com/blog/2016/07/28/structured-streaming-in-apache-spark.html) so you can work with Streaming DataFrames.
# MAGIC
# MAGIC ![Spark streaming concepts](https://raw.githubusercontent.com/neaorin/databricks-workshop/master/media/gsasg-spark-streaming-concepts.png)
# MAGIC
# MAGIC Sensors, IoT devices, social networks, and online transactions are all generating data that needs to be monitored constantly and acted upon quickly. As a result, the need for large-scale, real-time stream processing is more evident than ever before. There are there are four broad ways Spark Streaming is being used today:
# MAGIC
# MAGIC * Streaming ETL – Data is continuously cleaned and aggregated before being pushed into data stores.
# MAGIC * Triggers – Anomalous behavior is detected in real-time and further downstream actions are triggered accordingly. E.g. unusual behavior of sensor devices generating actions.
# MAGIC * Data enrichment – Live data is enriched with more information by joining it with a static dataset allowing for a more complete real-time analysis.
# MAGIC * Complex sessions and continuous learning – Events related to a live session (e.g. user activity after logging into a website or application) are grouped together and analyzed. In some cases, the session information is used to continuously update machine learning models.
# MAGIC
# MAGIC
# MAGIC ![Spark Streaming diagram](https://databricks.com/wp-content/uploads/2016/06/gsasg-spark-streaming-workflow.png)
# MAGIC
# MAGIC In this tutorial, you connect a data ingestion system with Azure Databricks to stream data into an Apache Spark cluster in near real-time. You set up data ingestion system using Azure Event Hubs and then connect it to Azure Databricks to process the messages coming through. To access a stream of data, you use Twitter APIs to ingest tweets into Event Hubs. Once you have the data in Azure Databricks, you can run analytical jobs to further analyze the data. 
# MAGIC
# MAGIC By the end of this tutorial, you would have streamed tweets from Twitter (that have the term "Azure" in them) and read the tweets in Azure Databricks.
# MAGIC
# MAGIC The following illustration shows the application flow:
# MAGIC
# MAGIC ![Azure Databricks with Event Hubs](https://docs.microsoft.com/en-us/azure/azure-databricks/media/databricks-stream-from-eventhubs/databricks-eventhubs-tutorial.png)
# MAGIC
# MAGIC This tutorial covers the following tasks:
# MAGIC
# MAGIC > * Create a Twitter app to access streaming data
# MAGIC > * Attach libraries for Event Hubs and Twitter API
# MAGIC > * Send tweets to Event Hubs
# MAGIC > * Read tweets from Event Hubs
# MAGIC > * Run Sentiment Analysis on the incoming tweets
# MAGIC
# MAGIC ## Create a Twitter application
# MAGIC
# MAGIC To receive a stream of tweets, you create an application in Twitter. Follow the instructions create a Twitter application and record the values that you need to complete this tutorial.
# MAGIC
# MAGIC 1. From a web browser, go to [Twitter Application Management](http://twitter.com/app), and select **Create New App**.
# MAGIC
# MAGIC     ![Create Twitter application](https://docs.microsoft.com/en-us/azure/azure-databricks/media/databricks-stream-from-eventhubs/databricks-create-twitter-app.png "Create Twitter application")
# MAGIC
# MAGIC 2. In the **Create an application** page, provide the details for the new app, and then select **Create your Twitter application**.
# MAGIC
# MAGIC     ![Twitter application details](https://docs.microsoft.com/en-us/azure/azure-databricks/media/databricks-stream-from-eventhubs/databricks-provide-twitter-app-details.png "Twitter application details")
# MAGIC
# MAGIC 3. In the application page, select the **Keys and Access Tokens** tab and copy the values for **Consume Key** and **Consumer Secret**. Also, select **Create my access token** to generate the access tokens. Copy the values for **Access Token** and **Access Token Secret**.
# MAGIC
# MAGIC     ![Twitter application details](https://docs.microsoft.com/en-us/azure/azure-databricks/media/databricks-stream-from-eventhubs/twitter-app-key-secret.png "Twitter application details")
# MAGIC
# MAGIC Save the values that you retrieved for the Twitter application. You need the values later in the tutorial.
# MAGIC
# MAGIC ## Attach libraries to Spark cluster
# MAGIC
# MAGIC In this tutorial, you will make use of some external Python libraries. You will need the [Python Twitter library](https://github.com/tweepy/tweepy) to send tweets to Event Hubs. You also use the [Azure Event Hubs / Service Bus client library for Python](https://pypi.python.org/pypi/azure-servicebus/0.21.1) to read and write data into Azure Event Hubs. To use these APIs as part of your cluster, add them as libraries to Azure Databricks and then associate them with your Spark cluster. The following instructions show how to add the library to the **Shared** folder in your workspace.
# MAGIC
# MAGIC 1.  In the Azure Databricks workspace, select **Workspace**, and then right-click **Shared**. From the context menu, select **Create** > **Library**.
# MAGIC
# MAGIC     ![Add library dialog box](https://docs.microsoft.com/en-us/azure/azure-databricks/media/databricks-stream-from-eventhubs/databricks-add-library-option.png "Add library dialog box")
# MAGIC
# MAGIC 2. In the New Library page, for **Source** select **Upload Python Egg or PyPI**. For **PyPI Name**, enter the name for the package you want to add. Here are the package names for the libraries used in this tutorial:
# MAGIC
# MAGIC     * Service Bus / Event Hubs library - `azure-servicebus`
# MAGIC     * Twitter API - `tweepy`
# MAGIC     * Sentiment Analysis - `textblob`
# MAGIC
# MAGIC     ![Provide package name](https://github.com/neaorin/databricks-workshop/blob/master/media/databricks-library-1.png?raw=true "Provide Maven coordinates")
# MAGIC
# MAGIC 3. Select **Install Library**.
# MAGIC
# MAGIC 5. On the library page, select the cluster where you want to use the library. Once the library is successfully associated with the cluster, the status immediately changes to **Attached**.
# MAGIC
# MAGIC     ![Attach library to cluster](https://github.com/neaorin/databricks-workshop/blob/master/media/databricks-library-2.png?raw=true "Attach library to cluster")
# MAGIC
# MAGIC 6. Repeat these steps for all the packages listed under step 2.
# MAGIC
# MAGIC ## Create an Azure Event Hubs namespace
# MAGIC
# MAGIC 1. Log on to the Azure portal, and click **Create a resource** at the top left of the screen.
# MAGIC 1. Click **Internet of Things**, and then click **Event Hubs**.
# MAGIC    
# MAGIC     ![](https://docs.microsoft.com/en-us/azure/event-hubs/media/event-hubs-create/create-event-hub9.png)
# MAGIC 1. In **Create namespace**, enter a namespace name. The system immediately checks to see if the name is available.
# MAGIC    
# MAGIC     ![](https://docs.microsoft.com/en-us/azure/event-hubs/media/event-hubs-create/create-event-hub1.png)
# MAGIC 1. After making sure the namespace name is available, choose the pricing tier (Basic or Standard). Also, choose an Azure subscription, resource group, and location in which to create the resource. 
# MAGIC 1. Click **Create** to create the namespace. You may have to wait a few minutes for the system to fully provision the resources.
# MAGIC 2. In the portal list of namespaces, click the newly created namespace.
# MAGIC 2. Click **Shared access policies**, and then click **RootManageSharedAccessKey**.
# MAGIC     
# MAGIC     ![](https://docs.microsoft.com/en-us/azure/event-hubs/media/event-hubs-create/create-event-hub7.png)
# MAGIC
# MAGIC 3. Click the copy button to copy the **RootManageSharedAccessKey** connection string to the clipboard. Save this connection string in a temporary location, such as Notepad, to use later.
# MAGIC     
# MAGIC     ![](https://docs.microsoft.com/en-us/azure/event-hubs/media/event-hubs-create/create-event-hub8.png)
# MAGIC
# MAGIC ## Create an event hub
# MAGIC
# MAGIC 1. In the Event Hubs namespace list, click the newly created namespace.      
# MAGIC    
# MAGIC     ![](https://docs.microsoft.com/en-us/azure/event-hubs/media/event-hubs-create/create-event-hub2.png) 
# MAGIC
# MAGIC 2. In the namespace blade, click **Event Hubs**.
# MAGIC    
# MAGIC     ![](https://docs.microsoft.com/en-us/azure/event-hubs/media/event-hubs-create/create-event-hub3.png)
# MAGIC
# MAGIC 1. At the top of the blade, click **Add Event Hub**.
# MAGIC    
# MAGIC     ![](https://docs.microsoft.com/en-us/azure/event-hubs/media/event-hubs-create/create-event-hub4.png)
# MAGIC 1. Type a name for your event hub, then click **Create**.
# MAGIC    
# MAGIC     ![](https://docs.microsoft.com/en-us/azure/event-hubs/media/event-hubs-create/create-event-hub5.png)
# MAGIC
# MAGIC Your event hub is now created, and you have the connection strings you need to send and receive events.
# MAGIC
# MAGIC
# MAGIC ## Send tweets to Event Hub
# MAGIC
# MAGIC In the Python code cell below, replace the placeholders with values for your Event Hubs namespace and Twitter application that you created earlier. Then, run the cell.

# COMMAND ----------

from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener
from azure.servicebus import ServiceBusService
import json
import time

# Twitter access credentials
twitter_consumer_key = '(enter)'
twitter_consumer_secret = '(enter)'
twitter_access_token = '(enter)'
twitter_access_secret = '(enter)'

# Azure Event Hubs access credentials
event_hubs_namespace = '(enter)'
event_hubs_name = '(enter)'
event_hubs_key_name = '(enter)'
event_hubs_key_value = '(enter)'
 
auth = OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
auth.set_access_token(twitter_access_token, twitter_access_secret)

sbs = ServiceBusService(service_namespace=event_hubs_namespace, shared_access_key_name=event_hubs_key_name, shared_access_key_value=event_hubs_key_value)
 
class MyListener(StreamListener):
 
    def on_data(self, data):
        tweet = json.loads(data)
        if tweet.get('retweeted') == False and tweet.get('lang') == 'en':
          timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet.get('created_at'),'%a %b %d %H:%M:%S +0000 %Y'))
          event = {'created_at': tweet.get('created_at'), 'timestamp': timestamp, 'text': tweet.get('text')}
          print(event)       
          sbs.send_event(event_hubs_name, json.dumps(event))
        return True
 
    def on_error(self, status):
        print(status)
        return True
 
twitter_stream = Stream(auth, MyListener())
twitter_stream.filter(track=['#Azure'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Use the next notebook to process the tweets with Spark Streaming
# MAGIC
# MAGIC The code above streams tweets with the keyword "Azure" into Event Hubs in real time.
# MAGIC
# MAGIC Leave this code running (it's an endless loop), and head over to the second notebook in this tutorial, **2.ProcessTweets**, to set up Spark Streaming and process the tweets.