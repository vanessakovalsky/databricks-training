# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Moteur de recommandation de film utilisant du filtrage collaboratif
# MAGIC
# MAGIC Le [Filtrage Collaboratif (Collaborative filtering)](https://fr.wikipedia.org/wiki/Collaborative_filtering) est souvent utilisé dans les systèmes de recommandations. L'idée est que si vous avez un largene panel de préférence utilisateur, vous pouvez utiliser les techniques de filtres collaboratifs pour prévoir ce qu'il manque dans les préférences utilisateurs. Par exemple, vous avez l'historique de revue des files pour chaque utilisateurs sur votre site. Vous povuez utiliser le filtrage collaboratif pour recommander quel filme l'utilisateur pourrait aimé après.
# MAGIC
# MAGIC Voici un exemple sur comment créer une Matrice de factorisation via Alternating Least Square comme méthode de filtrage collaboratif. Cette méthode done un des meilleurs résultats sur un ensemble de données Netflix. L'exemple utilise [le support Spark ML's pour ALS](https://spark.apache.org/docs/2.2.0/ml-collaborative-filtering.html) sur [l'ensemble de données MovieLens](https://grouplens.org/datasets/movielens/).
# MAGIC
# MAGIC ## ALS: une présentation courte 
# MAGIC
# MAGIC Vous pouvez regarder cette courte vidéo [disponible sur YouTube](https://www.youtube.com/watch?v=FgGjc5oabrA), qui explique comment ALS fonctionne.
# MAGIC
# MAGIC L'algorythme alternating least squares fournit du filtrage collaboratif entre les utilisateurs et les produits pour trouver quels produits le consammateur pourrait aimer, en se basant sur ces notes précédentes.
# MAGIC
# MAGIC Dans ce cas, l'algorythme ALS créera une matrice de tous les utilisateurs versus tous les films. La plupart des cellules de la matrice seront vide. Une cellule vide signifie que l'utilisateur n'a pas encore évaluer le film. L'algorythme ALS trouveral l'évaluation probable (prédiction), en se basant sur les similarités entres les évaluations des utilisateurs. L'algorythme utilise le calcul par carré moindre pour minimiser les errors d'estimations, et alterne entre résoudre des facteurs sur les films et résoudre des facteurs sur les utilisateurs.
# MAGIC
# MAGIC L'exemple trivial suivant donne une idée du problème à résoudre. Cependant, garder en tête que le problème général est plus difficile car la matrice a souvent de nombreuses valeurs manquantes.
# MAGIC
# MAGIC ![](https://github.com/hatv/dsx_SparkLessons/blob/master/als.png?raw=true)
# MAGIC
# MAGIC Trouver les valeurs manquante en utilisant la factorisation de matrice suppose que : 
# MAGIC
# MAGIC * Chaque utilisateur peut être décrit par des attributs k ou des fonctionnalité. Par exemple, la fonctionnalité 1 pourrait être un nombre qui dit combien chaque utilisateur aime les films de science fiction.
# MAGIC * Chaque item (film) peut être décrit par un ensemble d'anolygie d'attributs k ou de fonctionnalités. Pour faire suite à l'exemple précédente, la fonctionnalité 1 pour le film pourrait être un nombre disant combien un film est de la pure science-fiction.
# MAGIC * Si on multiplie chaque fonctionnalité de l'utilisateur au fonctionnalités correspondante du film et que l'on ajoute le tout, on aurait une bonne approximation pour evaluer si un utilisateur aimerait ce film.
# MAGIC
# MAGIC On peut définir une matrice de factorisation d'approxiation des attributs k de l'utilisateur en math en laissant un utilisateur uprendre la forme d'un vector x_u à dimension-k. En parallèle un item i peut être un vecteur y_i à dimension-k. La prédiction de l'évaluation de l'utilisateur u pour l'item i est juste un produit de leur deux vecteurs. 
# MAGIC
# MAGIC ![](https://cdn-images-1.medium.com/max/1600/1*C02BnJr9kV0NxTu3SzoXZA.png)
# MAGIC
# MAGIC où le r_ui représente notre prédiction d'évaluation r_ui, et y_i (x⊺_u) est assumé d'etre un vecteur en colonne. Ces vecteurs utilisateurs et item sont souvent appelés vecteurs latent ou à faible dimension embarqué dans la litterétaure. Les attributs k sont souvent appelés les **facteur latents**. 
# MAGIC
# MAGIC L'alogrythme ALS remplit d'abord aléatoirement la matrice utilisateur avec des valeurs, puis il optimise la valeur des films afin de minimisé les erreurs. Enfin, il définit les constantes de la matrice de films et optimise la valeur de la matrice utilisateur. Cet alternance entre les matrices pour optimiser est la raison du nom de "alternating". En partant d'un ensemble de facteurs utilisateur définis (par exemple, les valeurs de la matrice utilisateur), nous connaissons l'évaluation pour trouver les meilleurs valeurs pour les facteurs du film en utilisant l'optimisation écrite en bas du graphique. Puis nous "alternons" et prenons les meilleurs facteurs utilisateur définies pour les facteurs de film. Le schéma suivant montre la sortie de cet algoryhtme. 
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
# MAGIC # Entrainer notre modèle
# MAGIC
# MAGIC L'objectif fondamental du ML est de généralisé à partir d'instance de données pour entrainter des modèles. Nous voulons évaluer le modèle pour estimer la qualité du modèle de généralisation pru les données sur lequel le modèle n'a paas été entraine. Cependant, comme les futures instances ont des valeurs cibles inconnus et que nous nepouvons vérifier la véracité de nos prédictions pour de futures instances actuellement, nous avons besoin d'utiliser des données que nous avons déjà pour simuler les futures données/ Evaluer le modèles avec les même données qui ont été utilisés pour l'entrainement n'est pas utile, car cela rappel au modèle les données d'entrainement, au lieu de le généraliser à partir de celles-ci.
# MAGIC
# MAGIC Une stratégie commune est de prendre toutes les données classé, et de les partager entre ensemble d'entrainement et d'évaluation, généralement avec un ratio de 70-80 pourcent pour l'entrainement et 20-30 pourcents pour l'évaluation. Le système de machine leraning utilise les données d'entrainement pour entrainer les modèles pour définir les patterns, et utilise les données d'évaluation pour évaluer la qualité prédictive du modèle entrainé. Le système ML évalue la performance prédictive en comparant les prédictions de l'ensemble de données d'évaluations avec des vrai valeurs (connues comme vérité terrain) en utilisant une variété de métriques. Généralement, on utilise le "meilleur" modèle pour l'ensemble d'évaluation poru rendre les prédictions pour les instances futures pour lesquelles on ne connait pas la réponse cible.
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
# MAGIC Nous pouvons persister le résultat en utilisant un système externe.

# COMMAND ----------

# we can save the predictions for later use

userRecs.write.saveAsTable('userRecs', format='parquet', mode='overwrite')
movieRecs.write.saveAsTable('movieRecs', format='parquet', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Regardons un peu les facteurs latents des matrices. Souvenez-vous, ce sont les valeur qui reflètent pour chaque film / utilisateur l'entrainement ALS qui a appris des données.

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
# MAGIC ## Exercices
# MAGIC
# MAGIC 1. Pouvez vous améliorer le modèle en spécifiant des paramètres différent à l'entrainer ALS ? Voici quelques points que vous povuez essayer : 
# MAGIC   * [Train-Validation Split](https://spark.apache.org/docs/2.3.0/ml-tuning.html#train-validation-split) peut vous aider à construire une grille de paramètres pour l'entraineur (aussi appelé *hyperparameters*) qui peut être utilisé en combinaison avec ensemble de données prévus pour l'entrainement. Les hypers paramètres ALS sont :
# MAGIC     - rank = le nombre de facteur latent dans le modèle
# MAGIC     - maxIter = le nombre maximum d'itérations
# MAGIC     - regParam = le paramètre de régularisation
# MAGIC
# MAGIC   * [Cross-Validation](https://spark.apache.org/docs/2.3.0/ml-tuning.html#cross-validation) vous permettra d'entrainer le mod-le avec diffirentes combinaisons (appelés *folds*) des données d'entrée.
# MAGIC   
# MAGIC Si vous n'êtes pas sur de comment appliquer les idées ci-dessus sur ALS, [cette video YouTube](https://youtu.be/FgGjc5oabrA?t=330) a un exemple.