# Databricks notebook source
# MAGIC %md
# MAGIC # Lire Données - Fichiers CSV
# MAGIC
# MAGIC **Objectifs:**
# MAGIC - Commencer à travailler avec la documentation de l'API
# MAGIC - Introduire la classe `SparkSession` et les autres points d'entrées
# MAGIC - Introduire la classe `DataFrameReader`
# MAGIC - Lire les données depuis:
# MAGIC   * CSV sans un Schema.
# MAGIC   * CSV avec un Schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Pour commencer
# MAGIC
# MAGIC Exécuter les cellules suivantes pour mettre en place l'environnement

# COMMAND ----------

# MAGIC %run "./Includes/Utility-Methods"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Points d'entrées
# MAGIC
# MAGIC Notre point d'entrée pour les applications Spark est la classe `SparkSession`.
# MAGIC
# MAGIC An instance of this object is already instantiated for us which can be easily demonstrated by running the next cell:

# COMMAND ----------

print(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC It's worth noting that in Spark 2.0 `SparkSession` is a replacement for the other entry points:
# MAGIC * `SparkContext`, available in our notebook as **sc**.
# MAGIC * `SQLContext`, or more specifically it's subclass `HiveContext`, available in our notebook as **sqlContext**.

# COMMAND ----------

print(sc)
print(sqlContext)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Before we can dig into the functionality of the `SparkSession` class, we need to know how to access the API documentation for Apache Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Spark API

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Page d'accueil Spark API**
# MAGIC 0. Ouvrir un nouvel onglet de navigateur.
# MAGIC 0. Rechercher **Spark API Latest** ou **Spark API _x.x.x_** pour une version spécifique.
# MAGIC 0. Séléctionner **Spark API Documentation - Spark _x.x.x_ Documentation - Apache Spark**. 
# MAGIC 0. L'ensemble de documentation à utiliser dépend du langage que vous utiliser.
# MAGIC
# MAGIC Raccourci
# MAGIC   * <a href="https://spark.apache.org/docs/latest/api/" target="_blank">Spark API Documentation - Latest</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark API (Python)
# MAGIC
# MAGIC 0. Selectionner **Spark Python API**.
# MAGIC 0. Chercher la documentation : `pyspark.sql.SparkSession`.
# MAGIC   0. En haut à gauche saisissez **SparkSession** dans le champs de rechercher.
# MAGIC   0. Appuyez sur **[Entrer]**.
# MAGIC   0. Les résultats de recherche apparaissent sur la droite.
# MAGIC   0. Cliquer sur **pyspark.sql.SparkSession (Python class, in pyspark.sql module)**
# MAGIC   0. La documentation s'ouvre sur le panneau de droite.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) SparkSession
# MAGIC
# MAGIC Quelques fonctions utiles :
# MAGIC * `createDataSet(..)`
# MAGIC * `createDataFrame(..)`
# MAGIC * `emptyDataSet(..)`
# MAGIC * `emptyDataFrame(..)`
# MAGIC * `range(..)`
# MAGIC * `read(..)`
# MAGIC * `readStream(..)`
# MAGIC * `sparkContext(..)`
# MAGIC * `sqlContext(..)`
# MAGIC * `sql(..)`
# MAGIC * `streams(..)`
# MAGIC * `table(..)`
# MAGIC * `udf(..)`
# MAGIC
# MAGIC La fonction qui nous intéresse le plus est dans `SparkSession.read()` et renvoir un `DataFrameReader`.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) DataFrameReader
# MAGIC
# MAGIC Chercher dans la documentation `DataFrameReader`.
# MAGIC
# MAGIC Quelques fonctions utiles:
# MAGIC * `csv(path)`
# MAGIC * `jdbc(url, table, ..., connectionProperties)`
# MAGIC * `json(path)`
# MAGIC * `format(source)`
# MAGIC * `load(path)`
# MAGIC * `orc(path)`
# MAGIC * `parquet(path)`
# MAGIC * `table(tableName)`
# MAGIC * `text(path)`
# MAGIC * `textFile(path)`
# MAGIC
# MAGIC Méthodes de configuration:
# MAGIC * `option(key, value)`
# MAGIC * `options(map)`
# MAGIC * `schema(schema)`

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lire un fichier CSV sans schéma
# MAGIC
# MAGIC Nous allons commencer par lire un fichier texte très simple

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### La source de données
# MAGIC * Pour cet exercice, nous allons utilisé un fichier séparé par des tabulation appelé **pageviews_by_second.tsv** (fichier de 255 MB provenant de Wikipedia)
# MAGIC * Nous pouvons utiliser **&percnt;fs ls ...** pour voir le fichier sur le DBFS.

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/pageviews_by_second_example.tsv

# COMMAND ----------

# MAGIC %md
# MAGIC Nous pouvons utiliser **&percnt;fs head ...** pour récupérer les premières dizaines de caractères du fichiers.

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/pageviews_by_second_example.tsv

# COMMAND ----------

# MAGIC %md
# MAGIC Quelques points à noter ici:
# MAGIC * Le fichier a un entête.
# MAGIC * Le fichier est séparé par des tab (nous pouvons le déduire depuis l'extension du fichier et l'absence d'autre caractères entre chaque colonne).
# MAGIC * Les deux premières colonnes sont des chaines et la troisième un nombre.
# MAGIC
# MAGIC En connaissant ces détails, nous pouvons lire le fichier CSV
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etape #1 - Lire le fichier
# MAGIC Commençons avec le minimum en spécifiant le caractère tab comme délimiteur et l'emplacement du fichier:

# COMMAND ----------

# A reference to our tab-separated-file
csvFile = "/FileStore/tables/pageviews_by_second_example.tsv"

tempDF = (spark.read           # The DataFrameReader
   .option("sep", "\t")        # Use tab delimiter (default is comma-separator)
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cela garantit le déclenchement d'un job.
# MAGIC
# MAGIC Un "Job" est déclencher chaque fois que __nous appuyons sur le bouton__.
# MAGIC
# MAGIC Dans certains cas, __une action peut créer plusieurs jobs__ (plusieurs raisons de manipuler les données).
# MAGIC
# MAGIC Dans ce cas, le lecteur doit __"récuperer" a la première ligne__ du fichier pour déterminer combien de colonnes de données nous avons.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Nous pouvons structurer le `DataFrame` en exécutant la commande `printSchema()`
# MAGIC
# MAGIC Dans ce prints sur la console le nom de chaque colonne, son type de données et s'il est null ou non.
# MAGIC
# MAGIC ** *Note:* ** *Nous verrons les fonctions `DataFrame` dans un autre notebook.*

# COMMAND ----------

tempDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Nous pouvons voir depuis le schéma que :
# MAGIC * il y a trois colonnes
# MAGIC * le nom des colonnes est **_c0**, **_c1**, et **_c2** (les noms sont générés automatiquement)
# MAGIC * les trois colonnes sont des **strings**
# MAGIC * les trois colonnes sont des **nullable**
# MAGIC
# MAGIC Si nous regardons rapidement les données, nous pouvons voir que la ligne 1 contient les entêtes et non des données

# COMMAND ----------

display(tempDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etape #2 - Utiliser l'entête du fichier
# MAGIC
# MAGIC Ensuite, nous pouvons ajouter une option qui dit au lecteur que les données contiennent une entête et d'utiliser cet entête pour déterminer le nom des colonnes.
# MAGIC
# MAGIC ** *NOTE:* ** *Nous savons que nous avons une entête en nous basant sur ce que nous avons vu dans le head du fichier plus tôt.*

# COMMAND ----------

(spark.read                    # The DataFrameReader
   .option("sep", "\t")        # Use tab delimiter (default is comma-separator)
   .option("header", "true")   # Use first line of all files as header
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Quelques notes à propos de cet itération:
# MAGIC * encore une fois, seulement un job
# MAGIC * il y a trois colonnes
# MAGIC * les trois colonnes sont des strings
# MAGIC * les trois colonnes sont des nullable
# MAGIC * le noms des colonnes est défini: **timestamp**, **site**, et **requests** (ce que nous voulions faire)
# MAGIC
# MAGIC Un aperçu de la première ligne du fichier est tout ce que le lecteur a besoin pour déterminer le nombre de colonne et le nom de chaque colonne.
# MAGIC
# MAGIC Avant de continuer, noter la durée de l'appel précédent, il doit être en dessous de 3 secondes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etape #3 - Déduire le schéma
# MAGIC
# MAGIC Enfin, nous pouvons ajouter une options qui dit au lecteur de déduire le type de données de chaque colonne (c'est-à-dire le schema)

# COMMAND ----------

(spark.read                        # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("sep", "\t")            # Use tab delimiter (default is comma-separator)
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(csvFile)                   # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Résumé: Lire un CSV avec un schéma déduit
# MAGIC * nous avons toujours 3 colonnes
# MAGIC * les trois colonnes sont toujours **nullable**
# MAGIC * les trois colonnes ont leur propre nom
# MAGIC * deux jobs ont été exécuté (contrairement à l'exemple précédent)
# MAGIC * nos trois colonnes ont maintenant un types distincts de données :
# MAGIC   * **timestamp** == **timestamp**
# MAGIC   * **site** == **string**
# MAGIC   * **requests** == **integer**
# MAGIC
# MAGIC **Question:** Pourquoi il y a eu deux jobs ?
# MAGIC
# MAGIC **Question:** Combien de temps le dernier job a duré ?
# MAGIC
# MAGIC **Question:** Pourquoi cela a pris autant de temps ?
# MAGIC
# MAGIC Discuss...

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Lire un CSV avec un User-Defined Schema
# MAGIC
# MAGIC Cette fois nous allons lire le même fichier.
# MAGIC
# MAGIC La différence ici est que nous allons définir le schéma avant et espérons éviter l'exécution de job supplémentaires.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etape #1 Déclarer le schema.
# MAGIC
# MAGIC C'est juste une liste de nom de champt et de types de données

# COMMAND ----------

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

csvSchema = StructType([
  StructField("timestamp", StringType(), False),
  StructField("site", StringType(), False),
  StructField("requests", IntegerType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Etape #2 Lire dans nos données (et afficher le schema).
# MAGIC
# MAGIC Nous pouvons spécifier le schéma, ou plutôt le `StructType`, avec la commande `schema(..)`:

# COMMAND ----------

(spark.read                   # The DataFrameReader
  .option('header', 'true')   # Ignore line #1 - it's a header
  .option('sep', "\t")        # Use tab delimiter (default is comma-separator)
  .schema(csvSchema)          # Use the specified schema
  .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
  .printSchema()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review: Reading CSV w/ User-Defined Schema
# MAGIC * We still have three columns
# MAGIC * All three columns are **NOT** nullable because we declared them as such.
# MAGIC * All three columns have their proper names
# MAGIC * Zero jobs were executed
# MAGIC * Our three columns now have distinct data types:
# MAGIC   * **timestamp** == **string**
# MAGIC   * **site** == **string**
# MAGIC   * **requests** == **integer**
# MAGIC
# MAGIC **Question:** Why were there no jobs?
# MAGIC
# MAGIC **Question:** What is different about the data types of these columns compared to the previous exercise & why?
# MAGIC
# MAGIC **Question:** Do I need to indicate that the file has a header?
# MAGIC
# MAGIC **Question:** Do the declared column names need to match the columns in the header of the TSV file?
# MAGIC
# MAGIC Discuss...

# COMMAND ----------

# MAGIC %md
# MAGIC For a list of all the options related to reading CSV files, please see the documentation for `DataFrameReader.csv(..)`

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at some of the other details of the `DataFrame` we just created for comparison sake.

# COMMAND ----------

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', "\t")
  .schema(csvSchema)
  .csv(csvFile)
)
print("Partitions: " + str(csvDF.rdd.getNumPartitions()) )
printRecordsPerPartition(csvDF)
print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC Start the next lesson, [Reading Data - JSON]($./2.Reading%20Data%20-%20JSON)