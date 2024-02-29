# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Cluster Analysis of Customer Data
# MAGIC
# MAGIC In the context of customer segmentation, **cluster analysis** is the use of a mathematical model to discover groups of similar customers based on finding the smallest variations among customers within each group. These homogeneous groups are known as “customer archetypes” or “personas”.
# MAGIC
# MAGIC The goal of cluster analysis in marketing is to accurately segment customers in order to achieve more effective customer marketing via personalization. A common cluster analysis method is a mathematical algorithm known as k-means cluster analysis, sometimes referred to as scientific segmentation. The clusters that result assist in better customer modeling and predictive analytics, and are also are used to target customers with offers and incentives personalized to their wants, needs and preferences.
# MAGIC
# MAGIC The process is not based on any predetermined thresholds or rules. Rather, the data itself reveals the customer prototypes that inherently exist within the population of customers.
# MAGIC
# MAGIC ![](https://i.imgur.com/S65Sk9c.jpg)
# MAGIC
# MAGIC ## k-means	clustering
# MAGIC
# MAGIC k-means	attempts	to	partition	a	set	of	data	points	into	K	distinct	clusters	(where	K	is	an	input	parameter
# MAGIC for	the	model).
# MAGIC
# MAGIC More	formally,	k-means	tries	to	find	clusters	so	as	to	minimize	the	sum	of	squared	errors	(or	distances)
# MAGIC within	each	cluster.	This	objective	function	is	known	as	the	within	cluster	sum	of	squared	errors
# MAGIC (WCSS).
# MAGIC
# MAGIC It	is	the	sum,	over	each	cluster,	of	the	squared	errors	between	each	point	and	the	cluster	center.
# MAGIC Starting	with	a	set	of	K	initial	cluster	centers	(which	are	computed	as	the	mean	vector	for	all	data	points
# MAGIC in	the	cluster),	the	standard	method	for	K-means	iterates	between	two	steps:
# MAGIC
# MAGIC 1.	 Assign	each	data	point	to	the	cluster	that	minimizes	the	WCSS.	The	sum	of	squares	is	equivalent	to
# MAGIC the	squared	Euclidean	distance;	therefore,	this	equates	to	assigning	each	point	to	the	closest	cluster
# MAGIC center	as	measured	by	the	Euclidean	distance	metric.
# MAGIC 2.	 Compute	the	new	cluster	centers	based	on	the	cluster	assignments	from	the	first	step.
# MAGIC
# MAGIC The	algorithm	proceeds	until	either	a	maximum	number	of	iterations	has	been	reached	or	convergence	has
# MAGIC been	achieved.	Convergence	means	that	the	cluster	assignments	no	longer	change	during	the	first	step;
# MAGIC therefore,	the	value	of	the	WCSS	objective	function	does	not	change	either.
# MAGIC
# MAGIC ![](https://sandipanweb.files.wordpress.com/2017/03/kmeans3.gif?w=676)
# MAGIC
# MAGIC ## Training a k-means	model	on	the	MovieLens dataset
# MAGIC
# MAGIC We	will	train	a	model	for	both	the	movie	and	user	factors	that	we	generated	by	running	our
# MAGIC recommendation	model.
# MAGIC
# MAGIC We	need	to	pass	in	the	number	of	clusters	K	and	the	maximum	number	of	iterations	for	the	algorithm	to
# MAGIC run.	Model	training	might	run	for	less	than	the	maximum	number	of	iterations	if	the	change	in	the	objective
# MAGIC function	from	one	iteration	to	the	next	is	less	than	the	tolerance	level	(the	default	for	this	tolerance	is
# MAGIC 0.0001).
# MAGIC
# MAGIC Spark	ML's	k-means	provides	random	and	K-means	||	initialization,	with	the	default	being	K-means	||.	As
# MAGIC both	of	these	initialization	methods	are	based	on	random	selection	to	some	extent,	each	model	training	run
# MAGIC will	return	a	different	result.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC As an input, we're going to use the User Factors matrix we obtained by using ALS on the reviews data, during the previous exercise. Of course, if we had more data about our movie-going customers (like age, income, location etc) we could add that to the input dataset.
# MAGIC
# MAGIC First, k-means in SparkML requires the input to be part of a single [Vector](https://spark.apache.org/docs/2.3.0/mllib-data-types.html#local-vector) column, and our `userFactors` DataFrame has the factors as an Array. We will use a [Spark UDF](https://docs.databricks.com/spark/latest/spark-sql/udf-in-python.html) to change the features column to a Vector.

# COMMAND ----------

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import expr

toVec_udf = udf(lambda xs: Vectors.dense(xs), VectorUDT())

userFactors = spark.sql("SELECT * FROM userFactors") \
  .withColumn("featuresVec", toVec_udf("features")) \
  .drop("features") \
  .withColumnRenamed("featuresVec", "features")

display(userFactors)

# COMMAND ----------

print(KMeans().explainParams())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC It's time to train our k-means model. In this example we're attempting to cluster our customers into 6 clusters. Choosing the "right" number of clusters for k-means is an art in itself, and there are [multiple methods](http://www.sthda.com/english/articles/29-cluster-validation-essentials/96-determining-the-optimal-number-of-clusters-3-must-know-methods/) which can be applied, depending on your particular dataset.

# COMMAND ----------

kmeans = KMeans(featuresCol="features", k=6, seed=1, maxIter=25)
model = kmeans.fit(userFactors)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC The model is now trained, so we can use the `transform` function to cluster our input data.

# COMMAND ----------

movieClusters = model.transform(userFactors)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can now see the cluster each individual customer belongs to. We can use that information for targeted actions (for example, give all people in cluster 1 some special offer and examine the results)

# COMMAND ----------

display(movieClusters)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can see a graphical representation of the clusters for each pair of two inputs in the dataset. We can see that some pairs of values make for more distinct clusters, while others do not. This is a good way of determining which of your features were the most relevant for the clustering algorithm.

# COMMAND ----------

display(model, movieClusters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercises
# MAGIC
# MAGIC 1. In addition to users, you can also cluster items (in this case, movies). 
# MAGIC
# MAGIC Try it. Use the `movieFactors` table from the previous exercise as the input. 
# MAGIC
# MAGIC Is there a relationship between the cluster the movie is in and the year it was made? Try using a visualization to find out - a scatter plot might be useful.