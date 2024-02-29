# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Movie Recommendations Engine using Collaborative Filtering
# MAGIC
# MAGIC [Collaborative filtering](http://en.wikipedia.org/wiki/Collaborative_filtering) is commonly used in recommender systems. The idea is if you have a large set of item-user preferences, you use collaborative filtering techniques to predict missing item-user preferences. For example, you have the movie review history for every user on your website. You use collaborative filtering to recommend which movies a user might enjoy next.
# MAGIC
# MAGIC Below is an example of how to perform Matrix Factorisation via Alternating Least Squares as a method of Collaborative Filtering. This method gave one of the best results of a single method on the Netflix dataset. The example uses Spark ML's [support for ALS](https://spark.apache.org/docs/2.2.0/ml-collaborative-filtering.html) on the [MovieLens dataset](https://grouplens.org/datasets/movielens/).
# MAGIC
# MAGIC ## ALS: a short explanation
# MAGIC
# MAGIC You can check out a short video [available on YouTube](https://www.youtube.com/watch?v=FgGjc5oabrA), explaining how ALS works.
# MAGIC
# MAGIC The alternating least squares (ALS) algorithm provides collaborative filtering between users and products to find products that the customers might like, based on their previous ratings.
# MAGIC
# MAGIC In this case, the ALS algorithm will create a matrix of all users versus all movies. Most cells in the matrix will be empty. An empty cell means the user hasn't reviewed the movie yet. The ALS algorithm will fill in the probable (predicted) ratings, based on similarities between user ratings. The algorithm uses the least squares computation to minimize the estimation errors, and alternates between solving for movie factors and solving for user factors.
# MAGIC
# MAGIC The following trivial example gives you an idea of the problem to solve. However, keep in mind that the general problem is much harder because the matrix often has far more missing values.
# MAGIC
# MAGIC ![](https://github.com/hatv/dsx_SparkLessons/blob/master/als.png?raw=true)
# MAGIC
# MAGIC Finding the missing values by using matrix factorization assumes that:
# MAGIC
# MAGIC * Each user can be described by k attributes or features. For example, feature 1 might be a number that says how much each user likes sci-fi movies.
# MAGIC * Each item (movie) can be described by an analagous set of k attributes or features. To correspond to the above example, feature 1 for the movie might be a number that says how close the movie is to pure sci-fi.
# MAGIC * If we multiply each feature of the user by the corresponding feature of the movie and add everything together, this will be a good approximation for the rating the user would give that movie.
# MAGIC
# MAGIC We can turn our matrix factorization approximation of a k-attribute user into math by letting a user u take the form of a k-dimensional vector x_u. Similarly, an item i can be k-dimensional vector y_i. User u’s predicted rating for item i is just the dot product of their two vectors.
# MAGIC
# MAGIC ![](https://cdn-images-1.medium.com/max/1600/1*C02BnJr9kV0NxTu3SzoXZA.png)
# MAGIC
# MAGIC where r_ui hat represents our prediction for the true rating r_ui, and y_i (x⊺_u) is assumed to be a column (row) vector. These user and item vectors are often called latent vectors or low-dimensional embeddings in the literature. The k attributes are often called the **latent factors**.
# MAGIC
# MAGIC The ALS algorithm first randomly fills the users matrix with values and then optimizes the value of the movies such that the error is minimized. Then, it holds the movies matrix constant and optimizes the value of the user’s matrix. This alternation between which matrix to optimize is the reason for the “alternating” in the name. Given a fixed set of user factors (i.e., values in the users matrix), we use the known ratings to find the best values for the movie factors using the optimization written at the bottom of the figure. Then we “alternate” and pick the best user factors given fixed movie factors. The next figure shows the outline of this algorithm.
# MAGIC
# MAGIC ![](https://image.slidesharecdn.com/mllib-sfmachinelearning-140509003134-phpapp02/95/largescale-machine-learning-with-apache-spark-35-638.jpg?cb=1399596202)

# COMMAND ----------

#configuration options

dbutils.widgets.text("container_name", "sample-container", "Container name")
dbutils.widgets.text("storage_account_access_key", "YOUR_ACCESS_KEY", "Storage Access Key / SAS")
dbutils.widgets.text("storage_account_name", "STORAGE_ACCOUNT_NAME", "Storage Account Name")

# COMMAND ----------

#import the required libraries

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

#read the movie ratings data
spark.conf.set(
  "fs.azure.account.key."+dbutils.widgets.get("storage_account_name")+".blob.core.windows.net",
  dbutils.widgets.get("storage_account_access_key"))

ratingsLocation = "wasbs://"+dbutils.widgets.get("container_name")+"@" +dbutils.widgets.get("storage_account_name")+".blob.core.windows.net/ratings.csv"

ratings = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(ratingsLocation)

display(ratings)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Training our model
# MAGIC
# MAGIC The fundamental goal of ML is to generalize beyond the data instances used to train models. We want to evaluate the model to estimate the quality of its pattern generalization for data the model has not been trained on. However, because future instances have unknown target values and we cannot check the accuracy of our predictions for future instances now, we need to use some of the data that we already know the answer for as a proxy for future data. Evaluating the model with the same data that was used for training is not useful, because it rewards models that can “remember” the training data, as opposed to generalizing from it.
# MAGIC
# MAGIC A common strategy is to take all available labeled data, and split it into training and evaluation subsets, usually with a ratio of 70-80 percent for training and 20-30 percent for evaluation. The ML system uses the training data to train models to see patterns, and uses the evaluation data to evaluate the predictive quality of the trained model. The ML system evaluates predictive performance by comparing predictions on the evaluation data set with true values (known as ground truth) using a variety of metrics. Usually, you use the “best” model on the evaluation subset to make predictions on future instances for which you do not know the target answer.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![traintestloop](https://mapr.com/blog/predicting-loan-credit-risk-using-apache-spark-machine-learning-random-forests/assets/blogimages/creditmlcrossvalidation.png)

# COMMAND ----------

print(ALS().explainParams())

# COMMAND ----------

# split the data into train and test datasets
(train, test) = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
          coldStartStrategy="drop")

model = als.fit(train)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# COMMAND ----------

# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)
# Generate top 10 user recommendations for each movie
movieRecs = model.recommendForAllItems(10)

# Generate top 10 movie recommendations for a specified set of users
users = ratings.select(als.getUserCol()).distinct().limit(3)
userSubsetRecs = model.recommendForUserSubset(users, 10)
# Generate top 10 user recommendations for a specified set of movies
movies = ratings.select(als.getItemCol()).distinct().limit(3)
movieSubSetRecs = model.recommendForItemSubset(movies, 10)

# COMMAND ----------

display(userRecs)

# COMMAND ----------

display(movieRecs)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can persist the results to be used by external systems.

# COMMAND ----------

# we can save the predictions for later use

userRecs.write.saveAsTable('userRecs', format='parquet', mode='overwrite')
movieRecs.write.saveAsTable('movieRecs', format='parquet', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's have a look at the latent factor matrixes. Remember, these are the values relating to each movie / user the ALS trainer has learned from the data.

# COMMAND ----------

display(model.userFactors)

# COMMAND ----------

display(model.itemFactors)

# COMMAND ----------

#save the user and movie factors tables, for use in the next tutorial (clustering)

model.userFactors.write.saveAsTable('userFactors', format='parquet', mode='overwrite')
model.itemFactors.write.saveAsTable('movieFactors', format='parquet', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercises
# MAGIC
# MAGIC 1. Can you improve the model by specifying different parameters for the ALS trainer? Here are some things you can try:
# MAGIC   * [Train-Validation Split](https://spark.apache.org/docs/2.3.0/ml-tuning.html#train-validation-split) can help you build a grid of trainer parameters (also called *hyperparameters*) which are used in combination with your specified train-validation split of the dataset. The ALS hyperparameters are:
# MAGIC     - rank = the number of latent factors in the model
# MAGIC     - maxIter = the maximum number of iterations
# MAGIC     - regParam = the regularization parameter
# MAGIC
# MAGIC   * [Cross-Validation](https://spark.apache.org/docs/2.3.0/ml-tuning.html#cross-validation) will allow you to train the model with different combinations (called *folds*) of the input data.
# MAGIC   
# MAGIC If you're unsure how to apply the above ideas to ALS, [this YouTube video](https://youtu.be/FgGjc5oabrA?t=330) has an example.