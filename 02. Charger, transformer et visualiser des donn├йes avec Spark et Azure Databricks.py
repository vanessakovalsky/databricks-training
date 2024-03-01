# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Charger, transformer et visualiser des données avec  Spark et Azure Databricks
# MAGIC
# MAGIC Ce Notebook montre comment créer et requêter une table ou un DataFrame sur un Azure Blob Storage, et utiliser les visualisations sur les données.
# MAGIC
# MAGIC Les données utilisée dans cet exemple viennent de [MovieLens dataset](https://grouplens.org/datasets/movielens/).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Etape 1: Définir la localisation des données
# MAGIC
# MAGIC
# MAGIC Il y a deux manières dans Databricks pour lire depuis Azure Blobl Storage. Soit vous liser les données en utilisants les [account keys](https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html#azure-blob-storage) ou en lisant les données en utilisant les signatures d'accès partagées (SAS).
# MAGIC
# MAGIC Pour démarrer, nous définissons la location et le type de fichier. Nous pouvons le faire en utilisant les  [widgets](https://docs.databricks.com/user-guide/notebooks/widgets.html). Les Widgets nous permette de rendre paramètrable l'exécution d'un notebook entier. D'abord nous le créons, puis nous pouvons le référencer tout le long du notebook. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Etape 2: Lire les données
# MAGIC
# MAGIC Maintenant que nous avons spécifiés notre fichiers de métadonnées, nous pouvons créer un [DataFrame](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes). Noter que nous utilisons une *option* pour spécifier que nous voulons importer le schema depuis le fichier. Nous pouvons aussi explicitement définir un schema particulier si nous en avons déjà un.
# MAGIC
# MAGIC Comme nous lisons les fichiers depuis Azure Blob Storage, nous utiliserons le [protocole WASBS](https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html) pour se référer à cette localisation.
# MAGIC
# MAGIC Commençons par créer une pair de DataFrames en Python, pour travailler sur nos données *Movies* et *Reviews*.

# COMMAND ----------

from pyspark.sql.functions import split, explode, col, udf
from pyspark.sql.types import *

ratingsLocation = "dbfs:/FileStore/tables/ratings.csv"
moviesLocation = "dbfs:/FileStore/tables/movies.csv"

print("Ratings file location       " + ratingsLocation)
print("Movies file location        " + moviesLocation)

ratings = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(ratingsLocation)

movies = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .load(moviesLocation)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Etape 3 : Requêter les données
# MAGIC
# MAGIC Maintenant que nous avons créés nos DataFrames, nous pouvons faire des requêtes sur celles-ci. Par exemple, nous pouvons identifier les colonnes spécifique à sélectionner et à afficher au travers de Databricks.

# COMMAND ----------

display(ratings)

# COMMAND ----------

display(movies)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Nous allons effectuer un petit traitement sur les donnée en utilisant `pyspark.sql`, qui est le module Spark SQL module pour Python.

# COMMAND ----------

# transform the timestamp data column to a date column
# first we cast the int column to Timestamp
ratingsTemp = ratings \
  .withColumn("date", ratings.timestamp.cast("Timestamp")) 
  
# then, we cast Timestamp to Date
ratings = ratingsTemp \
  .withColumn("reviewDate", ratingsTemp.date.cast("Date")) \
  .drop("date", "timestamp")

# COMMAND ----------

display(ratingsTemp)

# COMMAND ----------

display(ratings)

# COMMAND ----------

# use a Spark UDF(user-defined function) to get the year a movie was made, from the title
# NOTE: since Spark UDFs can impact performance, it's a good idea to persist the result to disk, to avoid having to re-do the computation
def titleToYear(title):
  try:
    return int(title[title.rfind("(")+1:title.rfind(")")])
  except:
    return None

# register the above Spark function as UDF
titleToYearUdf = udf(titleToYear, IntegerType())

# add the movieYear column
movies = movies.withColumn("movieYear", titleToYearUdf(movies.title))

# explode the 'movies'.'genres' values into separate rows
movies_denorm = movies.withColumn("genre", explode(split("genres", "\|"))).drop("genres")

# join movies and ratings datasets on movieId
ratings_denorm = ratings.alias('a').join(movies_denorm.alias('b'), 'movieId', 'inner') 

display(ratings_denorm)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Etape 4: Créer une vue ou une table
# MAGIC
# MAGIC Si vous voulez faire des requêtes sur ces données en tant que table en utilisant la syntaxe SQL, vous pouvez l'enregistrer comme une *view* ou une table.

# COMMAND ----------

ratings.createOrReplaceTempView("ratings")
display(ratings)
movies.createOrReplaceTempView("movies")
ratings_denorm.createOrReplaceTempView("ratings_denorm")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Nous pouvons faire une reque sur une vue temporaire en utilisant Spark SQL. Noter comment nous utilions `%sql` pour requêter la vue depuis SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT movieId, userId, rating, reviewDate
# MAGIC FROM ratings

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT movieId FROM movies;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Cela équivaudrait à utiliser la fonction `spark.sql()` en Python:

# COMMAND ----------

result = spark.sql("SELECT movieId, userId, rating, reviewDate FROM ratings")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Nous pouvons aussi utiliser les fonctions d'aggregations SQL comme `COUNT`, `SUM`, o `AVG`.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT genre, COUNT(rating) as total_ratings, AVG(rating) AS average_rating 
# MAGIC FROM ratings_denorm
# MAGIC GROUP BY genre
# MAGIC ORDER BY total_ratings DESC;

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Nous pouvons voir le plan d'exécution de la dernière requête en utilisant le mot-clé `EXPLAIN`.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC EXPLAIN
# MAGIC SELECT genre, COUNT(rating) as total_ratings, AVG(rating) AS average_rating 
# MAGIC FROM ratings_denorm
# MAGIC GROUP BY genre
# MAGIC ORDER BY total_ratings DESC;

# COMMAND ----------

# MAGIC %md ### Etape 5: Visualisations
# MAGIC
# MAGIC La manière la plus simple de créer une visualisation dans Databricks est soit d'appeler `display(<dataframe-name>)`, ou de lancer une commande SQL comme décris par exemple celle-ci dessous. Puis vous pouvez cliquer sur l'icône de graphique ![](https://docs.databricks.com/_images/chart-button.png) en dessous du résultats pour afficher un graphique. 
# MAGIC
# MAGIC Vous pouvez aussi cliquer sur la flèche vers le bas à côté de l'icône de graphique ![](https://docs.databricks.com/_images/chart-button.png) pour choisir un autre type de graphique et cliquer sur  **Plot Options...** pour configurer le graphique.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT genre, COUNT(rating) as total_ratings, AVG(rating) AS average_rating 
# MAGIC FROM ratings_denorm
# MAGIC GROUP BY genre
# MAGIC ORDER BY average_rating DESC;

# COMMAND ----------

# MAGIC %md Essayons d'autres visualisations. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT rating from ratings

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT movieYear, genre, COUNT(*) as n FROM ratings_denorm 
# MAGIC GROUP BY movieYear, genre

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT title, COUNT(rating) as total_ratings, AVG(rating) AS average_rating 
# MAGIC FROM ratings_denorm
# MAGIC GROUP BY title
# MAGIC HAVING total_ratings > 30

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Etape 6: persister les données pour une utilisation future
# MAGIC
# MAGIC Enregistrer comme vue temporaire, ces données ne sont disponibles que dans ce notebook spécifique. Si vous voulez que d'autres utilisateurs puisse requêter cette table, vous pouvez aussi créer une table depuis le DataFrame.

# COMMAND ----------

ratings.write.saveAsTable('ratings_table', format='parquet', mode='overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Cette table sera persister même lors du démarrage du cluster et permet à différents utilisateurs dans différents notebook de requêter ces données
# MAGIC
# MAGIC Vous pouvez voir la table enregistrée dans le menu **Data** à gauche de l'écran.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Exercices
# MAGIC
# MAGIC En utilisant Spark SQL ou les outils de visualisations de Databricks (ou les deux), essayer de répondre aux questions suivantes :
# MAGIC
# MAGIC 1. Combien de films ont été produit par an ?
# MAGIC 2. Quel a été le meilleur film de chaque décénie (en se basant sur les notes des utilisateurs) ?