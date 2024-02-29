# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Une introduction détaillé à Apache Spark sur Databricks
# MAGIC
# MAGIC ** Bienvenue Databricks! **
# MAGIC
# MAGIC Ce notebook a pour but d'être la première étape dans votre process d'apprendtissage sur comment utiliser Apache Spark sur Databricks. Nous progresserons au travers des différents conceptes fondamentaux, des abstractions fondamentales et des outils à votre disposition. Ce notebook vous apprend les concepts fondamentaux et les bonnes pratique directement de ceux qui le connaissent le mieux.
# MAGIC
# MAGIC Commençons par définir ce qu'est Databricks. Databricks est une plateforme gérée pour exécuter Apache Spark - cela sgnifie que vous n'aurez pas besoin d'apprendre les concepts complexe de gestion de cluster ou à faire des tâches de maintenance fastidieuses pour utiliser Spark. Databrick fournit également des fonctionnalités qui aide les utilisateurs à être plus productif avec Spark. C'est une plateforme avec une interface en pointer / cliquer pour ceux qui préfère les interface graphique comme les data scientis ou les data analysts. Cependant, cette UI est accompagné d'une API sophistiqué pour ceux qui sont intéressé par les apsect d'automatisation de leur chargement de donnée avec des tâches automatisés. Pour correspondre aux besoins des entreprises, Databricks inclus aussi des fonctionnalités comme la gestion des droit d'accès basée sur le rôle et d'autre optimisations intelligentes qui améliore l'utilisabilité pour les utilisateurs mais qui réduisent aussi les coûts et la complexité pour les administrateurs.
# MAGIC
# MAGIC ** La série d'introduction détaillé (source) **
# MAGIC
# MAGIC Ce notebook est la traduction du premier notebook d'une série qui a pour but de vous aider à aller plus vite dans l'apprentissage d'Apache Spark. Ce notebook est idéal pour ceux qui ont peu ou pas d'expérience avec Spark. La série est aussi utile pour ceux qui ont de l'expérience avec SPAR mais ne sont pas habitués à utilisé des outils comme la création UDL et les pipelines de machine Learning. Les sources et les autres notebooks sont disponibles ici : 
# MAGIC
# MAGIC - [A Gentle Introduction to Apache Spark on Databricks](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/346304/2168141618055043/484361/latest.html)
# MAGIC - [Apache Spark on Databricks for Data Scientists](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/346304/2168141618055194/484361/latest.html)
# MAGIC - [Apache Spark on Databricks for Data Engineers](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/346304/2168141618055194/484361/latest.html)
# MAGIC
# MAGIC ## Terminologie Databricks
# MAGIC
# MAGIC Databricks a des concepts clés qu'il est important de comprendre. has key concepts that are worth understanding. Vous allez constater que nombreux de ceux-ci correspondent aux liens et icônes qui sont disponibles dans le menu de gauche. Ils définissent les outils fondamentaux que Databricks vous fournit en tant qu'ilitsateur. Ils sont disponibles à la fois dans l'interface web aussi bien que dans l'API REST.
# MAGIC
# MAGIC -   ****Workspaces****
# MAGIC     -   Les Workspaces vous permette d'organiser tout votre travail dans Databricks. Comme un dossier dans votre ordinateur, il vous permette de sauvegarder vos  ****notebooks**** et vos ****libraries**** et de les partager avec d'autres utilisateurs. Les workspaces ne sont pas connecté aux données et ne doivent pas être utilisés pour stocker des données. Ils servent seulement à stocker vos  ****notebooks**** et vos ****libraries**** que vous utiliser pour travailler et manipuler vos données.
# MAGIC     - Voici une vidéo sur la création d'un [workspace](https://youtu.be/Q551ECGpNSc)
# MAGIC -   ****Notebooks****
# MAGIC     -   Les Notebooks sont des ensembles de plusieurs blocs (cells) qui vous permettent d'éxecuter des commandes. Les blocs contiennent le code dans un des langages suivants: `Scala`, `Python`, `R`, `SQL`, ou `Markdown`. Les Notebooks ont un langage par défaut, mais chaque bloc peut avoir un langage différent. Cela est fait en incluant `%[language name]` au début du bloc. Par exemple `%python`. Nous verrons cette fonctionnalité rapidement. 
# MAGIC     -   Les Notebooks ont besoin d'être connecté à un ****cluster**** afin d'être capable d'éxécuter les commandes, ils ne sont cependant pas attaché définitivement à un cluster, ce qui permet de les partagés via le web ou de les téléchargés sur votre machine.
# MAGIC     -   Voici une vidéo de démonstration de [Notebooks](https://youtu.be/xtHcZVroK8Y).
# MAGIC     -   ****Dashboards****
# MAGIC         -   ****Dashboards**** peuvent être créé depuis les ****notebooks**** en tant que manière d'afficher le résultats des blocs sans le code qui a généré ce résultat. 
# MAGIC     - ****Notebooks**** peuvent aussi être planifié en tant que ****jobs**** en un seul clic soit pour lancer un pipeline de données, mettre à jour un modèle de machine learning ou mettre à jour un tableau de bord.
# MAGIC -   ****Libraries****
# MAGIC     -   Les Libraries sont les paquets ou modules qui fournissent des fonctionnalités supplémentaires dont vous avez besoin pour résoudre vos problèmatiques métiers. Ils peuvent être écrit en  Scala ou Java jars; Python eggs ou avec des paquets personnalisés. Vous pouvez écrire et envoyer ces fichier manuellement ou vous pouvez les installer directement via un utilitaire gestionnaire de paquets comme pypi ou maven.
# MAGIC -   ****Tables****
# MAGIC     -   Les Tables sont des données structurés que vous et votre équipe utiliser pour votre analyses. Les tables existes à différents encroits. Elles peuvent être stocké sur un stockage cloud, elles peuvent être stocker sur le cluster que vous utlisez ou mise en cache en mémoire. [For more about tables see the documentation](https://docs.azuredatabricks.net/user-guide/tables.html).
# MAGIC -   ****Clusters****
# MAGIC     -   Les Clusters sont les groupes d'ordinateurs qui agissent comme un seul ordinateur. Dans Databricks, cela signifie que vous pouvez traiter 20 ordinateurs comme vous traiter un seul ordinateur. Les clusters permettent l'exécution du code depuis les  ****notebooks**** ou les ****libraries**** sur un ensemble de données. Ces données peuvent être des données brutes localisés sur un stockage cloud ou des données structurés chargée en tant que ****table**** sur le cluster sur lequel vous travailler.. 
# MAGIC     - Il est importer de noter que les clusters ont des accès de contrôle qui leurs permettent de vérifier qui a accès à chaque cluster.
# MAGIC     - Voici une vidéo de démonstration des [Clusters](https://youtu.be/67DeQOWIA7c).
# MAGIC -   ****Jobs****
# MAGIC     -   Les Jobs sont les outils qui permettent de planifier l'éxecution sur un  ****cluster**** déjà existant ou sur un cluster externe. Cela peut être des ****notebooks**** ou bien des jars ou des scripts Python. Ils peuvent être crée soit manuellement soit via l'API REST.
# MAGIC     -  Voici une vidéo de démonstration des [Jobs](<http://www.youtube.com/embed/srI9yNOAbU0).
# MAGIC -   ****Apps****
# MAGIC     -   Les Apps sont l'intégration de tiers applicatifs avec la plateforme Databricks. Cela inclut des applications comme Tableau.
# MAGIC
# MAGIC ## Les ressources d'aide de Databricks
# MAGIC
# MAGIC Databricks fournit une varité d'outils pour vous aider à apprendre comment utiliser Databricks et Apache Spark. Databricks héberge la plus grande collection de documentation pour Apache Spark disponible sur le web. .
# MAGIC
# MAGIC Pour accéder aux différentes ressource, cliquer sur le bouton du point d'interrogation en bas à gauche. Un menu s'affiche avec la documentation suivante :
# MAGIC
# MAGIC ![img](dbfs:/FileStore/shared_uploads/v.david@kovalibre.com/Capture_d_écran_du_2021_12_07_11_34_05.png)
# MAGIC
# MAGIC
# MAGIC -   ****Help center****
# MAGIC     -   Documentation officielle de Databricks, contenant également la documentation de Apache Spar
# MAGIC -   ****Release note****
# MAGIC     -   Liste des évolutions de l'outil au travers des versions
# MAGIC -   ****Documentation****
# MAGIC     -   Documentation officielle de Azure Databricks
# MAGIC -   ****Knowledge base****
# MAGIC     -   Liste des problèmes fréquents rencontrés et des solutions possibles
# MAGIC -   ****Databricks status****
# MAGIC     -   Etat des différentes briques de Databricks en fonction de localisation de Azure
# MAGIC -   ****Feedbacks****
# MAGIC     -   Possibilité de soumettre des propositions d'évolutions à Microsft Azure
# MAGIC     
# MAGIC ## Abstractions Databricks et Apache Spark
# MAGIC
# MAGIC Maintenant que nous avons définis la terminologie et les ressources, continuons avec une introcuction basique à Apache Stark et Databrick. Même si vous êtes familier avec les conceptes de Spark, prenons un moment pour nous assurer que nous partageons les mêmes définitions et vous donner l'opportunité d'en apprendre un peu plus sur l'historique de Spark.
# MAGIC
# MAGIC ### L'histoire du projet Apache Spark
# MAGIC
# MAGIC Spark a été écrit à l'origine par les fondateurs de Databricks pendant leur études à l'UC Berkeley. Le projet Spark a démarré en 2009, a été libéré en 2010, et en 2013, son code a été donné à Apache, devenant Apache Spark. Les employées de Databricks ont écrit environ 79% du code de Apache Spark et on contribuer plus de 10 fois plus que n'importe quelle autre organisation. Apache Spark est un frameworkd distributé de calcul pour l'exécution de code en parallèle au travers de nombreuses machines différentes. Tandis que les abstrations et les interfaces sont simple, la gestion des clusters d'ordinateur et le maintien de la stabilité pour la production ne l'est pas. Databricks fournit une solution Big Data simple en founrissant une solution d'hébergement d'Apache Spark.
# MAGIC
# MAGIC ### Le Contexte/Environment
# MAGIC
# MAGIC Voyons maintenant les abstractions du coeur d'Apache Spark pour s'assurer que vous êtes à l'aise avec toutes les pièces que vous avez besoin de comprendre pour utiliser Databricks et Spark efficacement. 
# MAGIC
# MAGIC Historiquement, Apache Spark a deux contextes de coeur qui sont disponibles pour l'utilisateur. Le `sparkContext` disponible en tant que `sc` et le `SQLContext` disponible en tant que `sqlContext`, ces deux contextes fournissent différentes fonctions et informations à l'utilisateur. Le contexte `sqlContext` fournit de nombreuses fonctionnalités de DataFrame alors que le contexte `sparkContext` se concentre plus sur le moteur Apache Spark lui-même.
# MAGIC
# MAGIC Cependant depuis Apache Spark 2.X, il n'y a plus qu'un seul contexte - le `SparkSession`.
# MAGIC
# MAGIC
# MAGIC # Démarrons avec un peu de code!
# MAGIC
# MAGIC Nous avons déjà abordé pas mal de poit. Nous alons maintenant passer à la démonstration pour voir la puissance d'Apache Spark et de Databricks ensemble. Pour cela, il est nécessaire d'importer ce notebook dans votre propre environnement. Vous pouvez alors Importer ce notebook via un clic droit dans votre workspace. Ou bien , si vous préférez, vous pouvez retaper l'ensemble des commandes vous-mêmes, en créant un nouveau notebook virege et en tapant les commandes ci-dessous.
# MAGIC
# MAGIC
# MAGIC ## Créér ou lancer un Cluster
# MAGIC
# MAGIC Cliquer sur le bouton Compute, puis sur la page des Cluster, 
# MAGIC
# MAGIC - si vous avez déjà un cluster, assurez vous qu'il soit démarré ou lancer le avec le bouton start
# MAGIC - si vous n'avez pas encore de cluster cliquer sur ![img](https://training.databricks.com/databricks_guide/create_cluster.png) .
# MAGIC
# MAGIC Puis entrer les paramètre de configuration dans la fenête Create Cluster.
# MAGIC
# MAGIC Enfin, 
# MAGIC
# MAGIC -   Selectionner un nom unique pour le CLuster.
# MAGIC -   Selectionner la version du Runtime (laisser la dernière LTS par défaut ou utiliser la dernière versionà.
# MAGIC -   Entrer le nombre de worker à mettre, minimum 1, maximum 2.
# MAGIC
# MAGIC Pour en savoir plus sur les options disponibles pour les cluster voir [Databricks Guide on Clusters](https://docs.azuredatabricks.net/user-guide/clusters/index.html).

# COMMAND ----------

# MAGIC %md Commençons par explorer la session mentionné auparavant `SparkSession`. Nous pouvons y accéder via la variable `spark`. Comme expliqué, la sessio Spark est la localisation de base où Apache Stark stocke les informations sur son coeur.. 
# MAGIC
# MAGIC Les Cells peuvent être exécutée en appuyant sur `shift+enter` lorsque la cellule est sélectionné ou en appuyant sur la fleche en haut à droite de la cellule.

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md Nous pouvons utiliser le Contexte Spark pour accéder aux information mais nous pouvons aussi parallelliser une collection. Ici nou parallellisions un petit range en Python qui fournit un retour de type `DataFrame`.

# COMMAND ----------

firstDataFrame = spark.range(1000000)

# The code for 2.X is
# spark.range(1000000)
print(firstDataFrame)

# COMMAND ----------

# MAGIC %md Certains peuvent penser que cela afficherait la valeur de la  `DataFrame` qui a été parallelisé, mais ce n'est pas ainsi que Apache Spark fonctionne. Spark permet deux types d'opérations à l'utilisateurs. Il y a les  **transformations** et il y a les **actions**.
# MAGIC
# MAGIC ### Transformations
# MAGIC
# MAGIC Les Transformations sont les opérations qui ne seront pas complété pendant que vous écriver et éxecuté du code dans une cellule - elles seronts seulement exécutés lorsque vous appelée une **action**. Une exemple de transformation pourrait être la conversion d'un entier en float ou le filtre d'un ensemble de valeurs.
# MAGIC
# MAGIC ### Actions
# MAGIC
# MAGIC Les Actions sont les commandes qui sont exécuté par Spark pendant le temps de leur exécution. Elles consistes à lancer toutes les transformations précédentes afin d'obtenir un résultat. Une action est composé d'un ou plusieurs jobs qui consistes en des tâches qui sont exécutées par le worker en parallèle lorsque cela est possible
# MAGIC
# MAGIC Voici des exemples dimple de transformations et d'actions. Rappelez-vous, ce **ne sont pas toutes** les transformations et actions - c'est un échantillon de celle-ci. Nous aborderons rapidement pour Apache Spark est conçu de cette manière!
# MAGIC
# MAGIC ![transformations and actions](https://training.databricks.com/databricks_guide/gentle_introduction/trans_and_actions.png)

# COMMAND ----------

# An example of a transformation
# select the ID column values and multiply them by 2
secondDataFrame = firstDataFrame.selectExpr("(id * 2) as value")

# COMMAND ----------

# an example of an action
# take the first 5 values that we have in our firstDataFrame
print(firstDataFrame.take(5))
# take the first 5 values that we have in our secondDataFrame
print( secondDataFrame.take(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Maintenant que nous avons vu que Spark consiste en des actions et des transformations, parlons de pourquoi c'est le cas. La raison pour cela est que c'est un moyen simple d'optimiser la chaine complète de cacul à l'opposé des pièces individuelles. Cela permet d'être très rapide pour certains type de calcul car ils peuvent tous être fait en une seule fois. Techniquement les  `pipelines` Spark fonot ces calculs, comme on peut le voir dans l'image ci-dessous. Cela fisnigie que certains calculs sont faite en une seule fois (comme le mapping et le filtre) plutôt que de faire les opérations les unes après les autres pour l'ensemble des données.
# MAGIC
# MAGIC ![transformations and actions](https://training.databricks.com/databricks_guide/gentle_introduction/pipeline.png)
# MAGIC
# MAGIC Apache Spark peut aussi garder le résultat en mémoire à l'opposé d'autres framework qui écrivent immédiatement sur le disque après chaque tâche.
# MAGIC
# MAGIC ## Architecture de Apache Spark
# MAGIC
# MAGIC Avant de continuer avec notre exemple, voyons un aperçu de l'architecture de Apache Spark. Comme dit précédemment, Apache Spark vous permet de traiter de nombreuses machines comme une seul et cela est possible via une architecture de type master-worker où il y a  `driver` ou noeud maitre, accompagné de noeuds `worker`. Le maitre envoit le travail aux worker et leur indique s'ils doivent récupérer les données depuis la mémoire ou depuis le disque (ou depuis une autre source de données).
# MAGIC
# MAGIC Le schéma ci-dessous montre un exemple de cluster Apache Spark, de base il y a un noeud Driver qui comminque avec des noeuds exécutants. Chacun de ses exécutants ont des créneaux qui sont logiquement comme des coeurs d'exécution. 
# MAGIC
# MAGIC ![spark-architecture](https://training.databricks.com/databricks_guide/gentle_introduction/videoss_logo.png)
# MAGIC
# MAGIC Le Driver envoie des Tasks à un créenau libre sur l'Executors lorsque le travail doit être fait:
# MAGIC
# MAGIC ![spark-architecture](https://training.databricks.com/databricks_guide/gentle_introduction/spark_cluster_tasks.png)
# MAGIC
# MAGIC Vous pouvez voir les détails de votre application Apache Spark dans l'interface graphique web de Apache Spark. L'interface graphique web est accessible dans Databrics en allant sur Compute, puis sur la ligne de votre Cluster et enfin en cliquant sur " Spark UI", elle est aussi accessible en cliquant en haut à gauche de ce notebook, où on selectionner le cluster à attacher au notebook/ Dans cette option il y a un lien vers Spark UI.
# MAGIC
# MAGIC A un niveau plus haut, l'application Apache Spark consiste en un programme de conduite qui lance des opérations diverse en parallèlles Java Virtual Machine (JVMs) qui sont lancer dans un cluster ou localement sur la même machine. Dans Databricks, l'interface notebook est le programme de conduite. Le programme de conduite contient la boucle principale du programe et créer des ensemble de données distribués sur le cluster, puis il applique les opérations (transformations et actions) à ces ensembles de données.
# MAGIC Le programme de conduite accède à Apache Spark au travers de l'objet `SparkSession` sans tenir compte de la localisatino du déploiement.
# MAGIC
# MAGIC ## Un exemple réel des  Transformations et Actions
# MAGIC
# MAGIC Pour illustrer cette architecture et surtout les **transformations** et **actions** - étudions un exemple plus complet, cette fois en utilisant `DataFrames` et un fichier csv. 
# MAGIC
# MAGIC Le DataFrame et SparkSQL vont nous aider à construire un plan pour accéder aux données et finalement exécute le plan avec une action. Nous verrons ce process dans le schéma ci-dessous. Nous allons suivre un process d'analyse des requêtes, de constructions d'un plan, de comparaisons et finalement d'exécution.
# MAGIC
# MAGIC ![Spark Query Plan](https://training.databricks.com/databricks_guide/gentle_introduction/query-plan-generation.png)
# MAGIC
# MAGIC Nous ne rentrerons pas dans les détails poussés sur comment ce process fonctionne, vous pouvez lire beaucoup d'informations sur ce process sur le [Databricks blog](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html). Pour ceux qui veulent en savoir plus sur Comment Apache Spark exécute ce process, il est fortement recommandé de lire ce blog!
# MAGIC
# MAGIC En allant plus loin, nous allons accéder à un ensemble de données public que Databricks rend disponibles. Les ensemble de données Databricks sont des petites parties des données que nous avons rassembles depuis le web. Nous les rendons disponibles en utilisant le système de fichier Databricks. Allons charger l'ensemble de données sur les diamants popupaire dans une `DataFrame` spark. Maintenant étudions cet ensemble de données et la manière de travailler avec.

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/datasets.csv

# COMMAND ----------

dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamonds = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(dataPath)
  
# inferSchema means we will automatically figure out column types 
# at a cost of reading the data more than once

# COMMAND ----------

# MAGIC %md Maintenant que nous avons chargé les données, nous allons faire des calculs sur celles-ci. Cela nous permet de faire un tour des fonctionnalités basiques et de quelques fonctionnalités qui rendent Spark sur Databricks très simple! Afin d'être capable de faire nos calcus, nous avons besoin de comprendre mieux nos données. Nous pouvons visualiser les données pour les comprendre avec la fonction `display`.

# COMMAND ----------

display(diamonds)

# COMMAND ----------

# MAGIC %md Ce qui rend `display` exceptionel est le fait que l'on peut très facilement crée des graphiques complexes en cliquant sur l'icone de graphique que vous voyez ci-dessous. Ici un graphique qui permet de comparer le prix, la couleur et la coupe.

# COMMAND ----------

display(diamonds)

# COMMAND ----------

# MAGIC %md Maintenant que nous avons exploiré les données, retournons à la compréhension des **transformations** et **actions**. Nous vous fournissons plusieurs transformations et une action. Ensuite nous inspecterons en détail ce qu'il se passe à chaque étape.
# MAGIC
# MAGIC Ces transformations sont simple, premièrement nous groupons deux variables, cut et color, puis nous calculons le prix moyen. Ensuite nous allons faire une jointure sur l'ensemble de donnée originale sur la colonne `color`. Puis nous selectionnera le prix moyen ainsi que le carat sur le nouvel ensemble de données.

# COMMAND ----------

df1 = diamonds.groupBy("cut", "color").avg("price") # a simple grouping

df2 = df1\
  .join(diamonds, on='color', how='inner')\
  .select("`avg(price)`", "carat")
# a simple join and selecting some columns

# COMMAND ----------

# MAGIC %md Ces transformations sont maintenant complète dans le sens où rien ne s'est passé. Comme vous le voyez ci-dessous, nous obtenons aucun résultats! 
# MAGIC
# MAGIC La raison à cela est que ces calculs sont *lazy* de sorte à construire le flux complet de donnés du début à la fin dont l'utilsiateur à besoin. C'est une optimisation intelligente pour deux raisons. Tous les calculs peuvent être recalculés depuis la dource original de données permettant à Apache Spark de gérer les échecs qui arrive sur le chemin, et de gérer aux mieux les lenteurs. Deuxièmement, Apache Spark peut optimiser les calculs de sorte à ce que les données et les calculs puissent être mise dans des `pipeline` comme dit précédemment. Cependant, pour chaque transformation Apache Spark crée un plan sur comment il va faire ce travail.
# MAGIC
# MAGIC Pour comprendre ce qu'est un plan, nous allons utiliser la méthode `explain`. Rappelez vous qu'aucun de nos calculs n'a encore été exécuté, donc tout ce que fait cette méthode explain est de nous dire l'estimation de comment le calcul se fera sur l'ensemble de données.

# COMMAND ----------

df2.explain()

# COMMAND ----------

# MAGIC %md Expliquer en détail de résultat ci-dessus est hors du périmètre de cet exercice, mais n'hésitez pas à vous renseigner sur les plans d'éexecution utilisé en SQL pour mieux comprendre. Ce que nous devons déduire est que Park a génér un plan sur comment il espère exécuter la requête. Nous allons maintenant lancer l'action pour éxécuter le plan ci-dessus.

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %md Cela exécutera le plan que Apache Spark a construit précédemment. Cliquer sur la petite flèche à côté de  `(2) Spark Jobs` après que la cell est finis l'execution puis cliquer sur le lien `View`. Cela montre l'interface web d'Apache Spark dans votre notebooks. On peut également accéder à cet écran depuis le bouton d'attachement du cluster en haut du notebook. D'ans l'interface de Spark, vous devriez voir quelque chose qui inclus un graphique ressemblant à celui-ci : 
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![img](https://training.databricks.com/databricks_guide/gentle_introduction/spark-dag-ui.png)
# MAGIC
# MAGIC Ce sont les graphiques significatifs. Ce sont des Directed Acyclic Graphs (DAG)s de tous les calculs qui ont été fait afin d'obtenir ce résultat.  
# MAGIC
# MAGIC Encore une fois, le DAG est génrés car les transformations sont *lazy* - lorsqu'il génère ces séries d'étapes Spark optime de nombreuxes chose sur le flux et génerera même du code. Avec les DataFrames et Datasets, Apache Spark fonctionnera en arrière plan pour optmiser le plan d'exécution de la requête complète et les étapes du pipeline complet. Vous verrez des instances de `WholeStageCodeGen` ainsi que de  `tungsten` dans les plans et cela quelque soit les améliorations [in SparkSQL which you can read more about on the Databricks blog.](https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html)
# MAGIC
# MAGIC Dans ce schéma vous voyez que nosu avons démarré avec un CSV tout le long à côté, fait des changements, fusionner avec un autre fichier CSV (celui qui a été créé depuis le Dataframe original), puis rassembler ces deux fichiers ensemble et enfin faire des aggregations jusqu'à obtenir notre résultat final!

# COMMAND ----------

# MAGIC %md ### Mise en cache
# MAGIC
# MAGIC Un des partie importante d'Apache Spark est sa capacité à stocker les choses en mémoire pendant le calcul. C'est un truc intéressant à utiliser pour accélrer votre acccès aux tables requêtées fréquemment ou à certaines parties de données. C'est aussi bien pour les algorythmes itératig qui parcoure encore et encore les mêmes données. Alors que certains vois cela comme la panacé à tous les problèmes de vitesse, voyez le plutôt comme un outil que vous pouvez utiliser. D'autres concepts important comment le partitionnement de données, la mise en cluster et en bucket peuvent avoir un meilleur effet sur l'exécution d'un job que la mise en cache - souvez-vous, ce sont tous des des outils dans votre panoplie d'outils.  
# MAGIC
# MAGIC Pour mettre en cache un Datafraime, utiliser simplement la méthode cache.

# COMMAND ----------

df2.cache()

# COMMAND ----------

# MAGIC %md La mise en cache, comme une transformation, est faite de manière paresseuse. Cela signifie que les donnée ne seront pas stockée en mémoire tant que vous n'appeler pas une action sur l'ensemble de données. 
# MAGIC
# MAGIC Voici un exempla simple. Nous avons crée notre Dataframe df2 qui est essentiellement un plan logique qui nous dit comment calculer sur ce Dataframe spécifique. Nous avons dit à APache Spark de mettre en cache les données après avoir calculer pour la première fois. Appelons un scan complet des données avec un count deux fois. La première fois, cela crée le Dataframe, le met en cache en mémoire, puis retourne le résultat. La seconde fois, plutôt que de reclaculer le DataFrame complet, il ira seulement chercher la version en mémoire/
# MAGIC
# MAGIC Voyons comment découvrir cela.

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %md Nous avons maintenant compter nos données; Nous verrons que le explain se termine légèrement différement.

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %md Dans l'exemple ci-dessus, nous pouvons voir que cela coupe le temps nécessaire à générer les données. Avec une analyse de données plus complexe et large, le gains serait encore plus important!

# COMMAND ----------

# MAGIC %md ## Conclusion
# MAGIC
# MAGIC Dans ce notebook, nous avons vu de nombreuses choses! Mais vous comprenez mieux Spark and Databricks. Maintenant que vous avez terminer, vous devriez être plus à l'aise avec les concepts principaux de Spark sur Databricks. 