# Databricks notebook source
# MAGIC %md
# MAGIC # Exercise 10 : MLFlow
# MAGIC
# MAGIC MLFlow provides end-to-end lifecycle management, such as logging (tracking), deploying model, and automating MLFlow project by MLFlow CLI. Databricks already includes managed MLFlow and you can easily integrate with your project in MLFlow.
# MAGIC
# MAGIC Here we run Exercise 04 with MLFlow integrated tracking.
# MAGIC
# MAGIC Before starting, do the following settings in your cluster.
# MAGIC
# MAGIC - Needs Databricks Runtime ML and use Runtime version 5.4 or above. (MLlib-MLFlow integrated tracking is supported in these runtime.)
# MAGIC - Download [flight_weather.csv](https://1drv.ms/u/s!AuopXnMb-AqcgbZD7jEX6OTb4j8CTQ?e=KkeDdT) in your blob container. (See "Exercise 01 : Storage Settings" for details.)
# MAGIC - Install MMLSpark to use LightGBM into your cluster. (See "Exercise 03 : Spark Machine Learning Pipeline" for installation.)
# MAGIC - Restart your cluster.
# MAGIC
# MAGIC > Note : Here we deploy Spark ML pipeline model for serving with MLFlow - Azure Machine Learning (Azure ML) integration capabilities.    
# MAGIC When you are not on Databricks, you can also deploy Spark ML pipeline model with only Azure Machine Learning. (See [here](https://tsmatz.wordpress.com/2019/03/04/spark-ml-pipeline-serving-inference-by-azure-machine-learning-service/) for details. Both architectures are different.)
# MAGIC
# MAGIC *back to [index](https://github.com/tsmatz/azure-databricks-exercise)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Train and Track Metric with MLFlow
# MAGIC
# MAGIC Almost all is the same in Exercise 04. But please see the following note with bold fonts

# COMMAND ----------

# Read dataset
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
df = (sqlContext.read.format("csv").
  option("header", "true").
  option("nullValue", "NA").
  option("inferSchema", True).
  load("abfss://container01@demostore01.dfs.core.windows.net/flight_weather.csv"))

# COMMAND ----------

# ARR_DEL15 = 1 if it's canceled.
from pyspark.sql.functions import when
df = df.withColumn("ARR_DEL15", when(df["CANCELLED"] == 1, 1).otherwise(df["ARR_DEL15"]))

# COMMAND ----------

# Remove flights if it's diverted.
df = df.filter(df["DIVERTED"] == 0)

# COMMAND ----------

# Select required columns
df = df.select(
  "ARR_DEL15",
  "MONTH",
  "DAY_OF_WEEK",
  "UNIQUE_CARRIER",
  "ORIGIN",
  "DEST",
  "CRS_DEP_TIME",
  "CRS_ARR_TIME",
  "RelativeHumidityOrigin",
  "AltimeterOrigin",
  "DryBulbCelsiusOrigin",
  "WindSpeedOrigin",
  "VisibilityOrigin",
  "DewPointCelsiusOrigin",
  "RelativeHumidityDest",
  "AltimeterDest",
  "DryBulbCelsiusDest",
  "WindSpeedDest",
  "VisibilityDest",
  "DewPointCelsiusDest")

# COMMAND ----------

# Drop rows with null values
df = df.dropna()

# COMMAND ----------

# Convert categorical values to indexer (0, 1, ...)
from pyspark.ml.feature import StringIndexer
uniqueCarrierIndexer = StringIndexer(inputCol="UNIQUE_CARRIER", outputCol="Indexed_UNIQUE_CARRIER").fit(df)
originIndexer = StringIndexer(inputCol="ORIGIN", outputCol="Indexed_ORIGIN").fit(df)
destIndexer = StringIndexer(inputCol="DEST", outputCol="Indexed_DEST").fit(df)
arrDel15Indexer = StringIndexer(inputCol="ARR_DEL15", outputCol="Indexed_ARR_DEL15").fit(df)

# COMMAND ----------

# Assemble feature columns
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(
  inputCols = [
    "MONTH",
    "DAY_OF_WEEK",
    "Indexed_UNIQUE_CARRIER",
    "Indexed_ORIGIN",
    "Indexed_DEST",
    "CRS_DEP_TIME",
    "CRS_ARR_TIME",
    "RelativeHumidityOrigin",
    "AltimeterOrigin",
    "DryBulbCelsiusOrigin",
    "WindSpeedOrigin",
    "VisibilityOrigin",
    "DewPointCelsiusOrigin",
    "RelativeHumidityDest",
    "AltimeterDest",
    "DryBulbCelsiusDest",
    "WindSpeedDest",
    "VisibilityDest",
    "DewPointCelsiusDest"],
  outputCol = "features")

# COMMAND ----------

# Define classifier
from mmlspark.lightgbm import LightGBMClassifier
classifier = LightGBMClassifier(
  featuresCol="features",
  labelCol="ARR_DEL15",
  numIterations=150)

# COMMAND ----------

# Create pipeline
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[uniqueCarrierIndexer, originIndexer, destIndexer, arrDel15Indexer, assembler, classifier])

# COMMAND ----------

# MAGIC %md
# MAGIC **Now we set ```metricName``` property in evaluator, and this metirc is logged by MLFlow.**

# COMMAND ----------

# Prepare training with ParamGridBuilder
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
### 4 x 2 = 8 times training occurs
paramGrid = ParamGridBuilder() \
  .addGrid(classifier.learningRate, [0.1, 0.3, 0.5, 0.7]) \
  .addGrid(classifier.numLeaves, [100, 200]) \
  .build()
tvs = TrainValidationSplit(
  estimator=pipeline,
  estimatorParamMaps=paramGrid,
  evaluator=MulticlassClassificationEvaluator(labelCol="ARR_DEL15", predictionCol="prediction", metricName="weightedPrecision"),
  trainRatio=0.8)  # data is separated by 80% and 20%, in which the former is used for training and the latter for evaluation

# COMMAND ----------

# MAGIC %md
# MAGIC **Start training with MLFlow tracking.** (The metric is tracked by ```mlflow.start_run()```.)    
# MAGIC Here we also log the best precision and save the best pipeline model file.
# MAGIC
# MAGIC (Use ```setParallelism()``` in ```TrainValidationSplit``` to improve performance. By default, it runs on a serial evaluation and takes a long time.)

# COMMAND ----------

# Start mlflow and run pipeline
import mlflow
from mlflow import spark
with mlflow.start_run():
  model = tvs.fit(df)                                               # logs above metric (weightedPrecision)
  mlflow.log_metric("bestPrecision", max(model.validationMetrics)); # logs user-defined custom metric
  mlflow.spark.log_model(model.bestModel, "model-file")             # logs model as artifacts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. See Logs with MLFlow UI
# MAGIC
# MAGIC Click "Runs" on top-right and click "View Experiment UI" in this notebook.
# MAGIC
# MAGIC ![Click Runs](https://tsmatz.github.io/images/github/databricks/20190620_View_Runs.jpg)
# MAGIC
# MAGIC MLFlow UI is displayed and you can view a top run (which includes above ```bestPrecision```) and derived 8 runs (each of which includes above ```weightedPrecision```) as follows. (total 9 runs)
# MAGIC
# MAGIC ![Experiment UI](https://tsmatz.github.io/images/github/databricks/20200423_experiment_ui.jpg)
# MAGIC
# MAGIC When you click the top level of runs, you can also see the saved artifacts (in this case, pipeline model) in MLFlow UI.
# MAGIC
# MAGIC ![MLFlow Artifacts](https://tsmatz.github.io/images/github/databricks/20200423_saved_artifacts.jpg)
# MAGIC
# MAGIC In experiment UI, set ```params.numLeaves = "200"``` in "Search Runs" box and click "Search" button. Then you can filter results and total 4 runs are listed as follows.    
# MAGIC Now select all filtered results (select all check-boxes) and click "Compare" button.
# MAGIC
# MAGIC ![Select and Compare](https://tsmatz.github.io/images/github/databricks/20200423_filter_compare.jpg)
# MAGIC
# MAGIC In the next screen, select ```learningRate``` on X-axis and ```weightedPrecision``` on Y-axis in scatter plot area.    
# MAGIC Then you can view the effect of ```learningRate``` for performance (accuracy) using a scatter plot as follows.
# MAGIC
# MAGIC ![Scatter Plot](https://tsmatz.github.io/images/github/databricks/20200423_scatter_plots.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. See Logs with Tracking API
# MAGIC Note : User **Databricks Runtime 5.1 and above**, because we show pandas dataframe using ```display()``` here.

# COMMAND ----------

all_runs = mlflow.search_runs(max_results=10)  # Note : This is pandas dataframe
display(all_runs)

# COMMAND ----------

# MAGIC %md
# MAGIC Transform run logs and show required visuals.

# COMMAND ----------

child_runs = all_runs.dropna(subset=["metrics.weightedPrecision"])
display(child_runs[["params.numLeaves","params.learningRate","metrics.weightedPrecision"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Track MLFlow logs in Azure Machine Learning
# MAGIC
# MAGIC You can also save your MLFlow logs in [Azure Machine Learning](https://tsmatz.wordpress.com/2018/11/20/azure-machine-learning-services/).
# MAGIC
# MAGIC Before starting, please prepare as follows.
# MAGIC
# MAGIC 1. Create new Azure Machine Learning in [Azure Portal](https://portal.azure.com/).    
# MAGIC After you've created your AML workspace, please copy your workspace name, resource group name, and subscription id.    
# MAGIC ![Copy attributes](https://tsmatz.github.io/images/github/databricks/20190710_AML_Workspace.jpg)
# MAGIC 2. Install Azure Machine Learning Python SDK for Databricks in your cluster as follows
# MAGIC     - On workspace, right-click "Shared". From the context menu, select "Create" > "Library"
# MAGIC     - Add ```azureml-mlflow``` with "PyPI" source
# MAGIC     - Installed library to your cluster
# MAGIC     - Restart your cluster
# MAGIC
# MAGIC > Note : Here (in this hands-on) we connect to an Azure Machine Learning workspace by running Python code, however, you can now use the following "Link Azure ML Workspace" button (simplified integrated experience) in Azure Databricks launcher page to connect a new or existing workspace. Once you have linked with this experience, you don't need to run the following ```ws.write_config()```, ```Workspace.from_config()```, and ```mlflow.set_tracking_uri()```.    
# MAGIC ![Link Azure ML Workspace](https://tsmatz.github.io/images/github/databricks/20200326_Link_AML.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following script **once** and write your config in your cluster.    
# MAGIC When you run the script, you should open https://microsoft.com/devicelogin and enter the code to authenticate along with the output.

# COMMAND ----------

# Run once in your cluster !
from azureml.core import Workspace
ws = Workspace(
  workspace_name = "<AML workspace name>",
  subscription_id = "<Azure Subscription Id>",
  resource_group = "<Resource Group name>")
ws.write_config()

# COMMAND ----------

# MAGIC %md
# MAGIC Once you've written your config in your cluster, you can run the following script to get AML workspace.

# COMMAND ----------

import mlflow
from azureml.core import Workspace
ws = Workspace.from_config()

# COMMAND ----------

# MAGIC %md
# MAGIC Set AML tracking uri in MLFlow to redirect your MLFlow logging into Azure Machine Learning.

# COMMAND ----------

tracking_uri = ws.get_mlflow_tracking_uri()
mlflow.set_tracking_uri(tracking_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC Set the expriment name for AML workspace. (As you can see later, AML logs are saved in each experiments.)

# COMMAND ----------

mlflow.set_experiment("databricks_mlflow_test")

# COMMAND ----------

# MAGIC %md
# MAGIC Run Spark ML trainig jobs with MLFlow logging.    
# MAGIC **All logs and metrics are transferred into Azure Machine Learning (Azure ML).**
# MAGIC
# MAGIC > Note : Here we've changed classifier to built-in SparkML ```DecisionTreeClassifier```, since MMLSpark ```LightGBMClassifier``` is not supported in ```mlflow.azureml.build_image()```. (The model accuracy will become lower than above.)

# COMMAND ----------

# Read dataset
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
df = (sqlContext.read.format("csv").
  option("header", "true").
  option("nullValue", "NA").
  option("inferSchema", True).
  load("abfss://container01@demostore01.dfs.core.windows.net/flight_weather.csv"))

# ARR_DEL15 = 1 if it's canceled.
from pyspark.sql.functions import when
df = df.withColumn("ARR_DEL15", when(df["CANCELLED"] == 1, 1).otherwise(df["ARR_DEL15"]))

# Remove flights if it's diverted.
df = df.filter(df["DIVERTED"] == 0)

# Select required columns
df = df.select(
  "ARR_DEL15",
  "MONTH",
  "DAY_OF_WEEK",
  "UNIQUE_CARRIER",
  "ORIGIN",
  "DEST",
  "CRS_DEP_TIME",
  "CRS_ARR_TIME",
  "RelativeHumidityOrigin",
  "AltimeterOrigin",
  "DryBulbCelsiusOrigin",
  "WindSpeedOrigin",
  "VisibilityOrigin",
  "DewPointCelsiusOrigin",
  "RelativeHumidityDest",
  "AltimeterDest",
  "DryBulbCelsiusDest",
  "WindSpeedDest",
  "VisibilityDest",
  "DewPointCelsiusDest")

# Drop rows with null values
df = df.dropna()

# Convert categorical values to indexer (0, 1, ...)
from pyspark.ml.feature import StringIndexer
uniqueCarrierIndexer = StringIndexer(inputCol="UNIQUE_CARRIER", outputCol="Indexed_UNIQUE_CARRIER").fit(df)
originIndexer = StringIndexer(inputCol="ORIGIN", outputCol="Indexed_ORIGIN").fit(df)
destIndexer = StringIndexer(inputCol="DEST", outputCol="Indexed_DEST").fit(df)
arrDel15Indexer = StringIndexer(inputCol="ARR_DEL15", outputCol="Indexed_ARR_DEL15").fit(df)

# Assemble feature columns
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(
  inputCols = [
    "MONTH",
    "DAY_OF_WEEK",
    "Indexed_UNIQUE_CARRIER",
    "Indexed_ORIGIN",
    "Indexed_DEST",
    "CRS_DEP_TIME",
    "CRS_ARR_TIME",
    "RelativeHumidityOrigin",
    "AltimeterOrigin",
    "DryBulbCelsiusOrigin",
    "WindSpeedOrigin",
    "VisibilityOrigin",
    "DewPointCelsiusOrigin",
    "RelativeHumidityDest",
    "AltimeterDest",
    "DryBulbCelsiusDest",
    "WindSpeedDest",
    "VisibilityDest",
    "DewPointCelsiusDest"],
  outputCol = "features")

# Define classifier
from pyspark.ml.classification import DecisionTreeClassifier
classifier = DecisionTreeClassifier(featuresCol="features", labelCol="ARR_DEL15")

# Create pipeline
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[uniqueCarrierIndexer, originIndexer, destIndexer, arrDel15Indexer, assembler, classifier])

# Prepare training with ParamGridBuilder
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import TrainValidationSplit
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
### 4 x 2 = 8 times training occurs (it takes a long time)
paramGrid = ParamGridBuilder() \
  .addGrid(classifier.maxDepth, [5, 10, 15, 20]) \
  .addGrid(classifier.maxBins, [251, 300]) \
  .build()
tvs = TrainValidationSplit(
  estimator=pipeline,
  estimatorParamMaps=paramGrid,
  evaluator=MulticlassClassificationEvaluator(labelCol="ARR_DEL15", predictionCol="prediction", metricName="weightedPrecision"),
  trainRatio=0.8)  # data is separated by 80% and 20%, in which the former is used for training and the latter for evaluation

# Start mlflow and run pipeline
from mlflow import spark
with mlflow.start_run():
  model = tvs.fit(df)                                               # logs above metric (weightedPrecision)
  mlflow.log_metric("bestPrecision", max(model.validationMetrics)); # logs user-defined custom metric
  mlflow.spark.log_model(model.bestModel, "model-file")             # logs model as artifacts

# COMMAND ----------

# MAGIC %md
# MAGIC Go to Azure Machine Learning in Azure Portal and click "Experiment" in the left-navigation. In a list of experiments, select one named "databricks_mlflow_test".    
# MAGIC You can see all metrics, including ```bestPrecision``` and ```weightedPrecision``` in all child runs.    
# MAGIC Now copy parent's Run ID.
# MAGIC
# MAGIC ![Azure ML Experiment UI](https://tsmatz.github.io/images/github/databricks/20200423_goto_experiment2.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC Set {Run ID} (which is copied above) in the following script and run.    
# MAGIC This script extracts ```maxDepth```, ```maxBins```, and ```weightedPrecision``` in all children runs from Azure Machine Learning workspace.

# COMMAND ----------

from azureml.core import Experiment
from azureml.core.run import Run
import pandas as pd
ml_parent_run = Run(Experiment(ws, "databricks_mlflow_test"), '<Run ID>')
ml_child_runs = ml_parent_run.get_children()
l = [[r.properties["mlflow.param.key.maxDepth"], r.properties["mlflow.param.key.maxBins"], r.get_metrics()["weightedPrecision"]] for r in ml_child_runs]
ml_df = pd.DataFrame(l, columns =["maxDepth", "maxBins", "weightedPrecision"])
display(ml_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Deploy Your Model
# MAGIC
# MAGIC Now we deploy Spark ML pipeline model for serving (inferencing web service) using MLFlow.    
# MAGIC Here we save a pipeline model as MLFlow model format, then generate container image for serving using ```mlflow.azureml.build_image()```.
# MAGIC
# MAGIC (With MLFlow, you can also load MLFlow model as generic Python functions via ```mlflow.pyfunc.load_model()```.)

# COMMAND ----------

# MAGIC %md
# MAGIC Save Spark ML pipeline model as MLFlow model format.

# COMMAND ----------

# mlflow.spark.log_model(model.bestModel, "best-model")  # You can also log model in Azure Machine Learning
mlflow.spark.save_model(model.bestModel, "/mnt/testblob/flight_mlflow_model")

# COMMAND ----------

# MAGIC %md
# MAGIC Register (upload) your model and create container image in Azure ML.

# COMMAND ----------

import mlflow.azureml
from azureml.core.webservice import AciWebservice, Webservice
registered_image, registered_model = mlflow.azureml.build_image(
  model_uri="/mnt/testblob/flight_mlflow_model",
  image_name="testimage",
  model_name="testmodel",
  workspace=ws,
  synchronous=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Deploy image and start web service.    
# MAGIC Here we use Azure Container Instance (ACI) for debugging purpose, but you can deploy using Azure Kubernetes Service (AKS) for production.

# COMMAND ----------

# create deployment config
aci_config = AciWebservice.deploy_configuration()
svc = Webservice.deploy_from_image(
  image=registered_image,
  deployment_config=aci_config,
  workspace=ws,
  name="testdeploy")
svc.wait_for_deployment(show_output=True)

# COMMAND ----------

# MAGIC %md
# MAGIC See details, if error has occured.

# COMMAND ----------

print(svc.get_logs())

# COMMAND ----------

# MAGIC %md
# MAGIC Show scoring (inference) Url.

# COMMAND ----------

svc.scoring_uri

# COMMAND ----------

# MAGIC %md
# MAGIC Call scoring (inference) service !

# COMMAND ----------

import requests
import json
headers = {'Content-Type':'application/json'}
body = {
  "columns": [
    "ARR_DEL15",
    "MONTH",
    "DAY_OF_WEEK",
    "UNIQUE_CARRIER",
    "ORIGIN",
    "DEST",
    "CRS_DEP_TIME",
    "CRS_ARR_TIME",
    "RelativeHumidityOrigin",
    "AltimeterOrigin",
    "DryBulbCelsiusOrigin",
    "WindSpeedOrigin",
    "VisibilityOrigin",
    "DewPointCelsiusOrigin",
    "RelativeHumidityDest",
    "AltimeterDest",
    "DryBulbCelsiusDest",
    "WindSpeedDest",
    "VisibilityDest",
    "DewPointCelsiusDest"
  ],
  "data": [
    [
      0,
      1,
      1,
      "DL",
      "ATL",
      "EWR",
      8,
      10,
      93.0,
      30.0175,
      12.05,
      7.5,
      1.0,
      11.025,
      96.5,
      30.055,
      4.5,
      6.0,
      0.5,
      3.95
    ],
    [
      0,
      1,
      1,
      "AA",
      "ABQ",
      "DFW",
      11,
      14,
      34.0,
      30.16,
      -2.8,
      15.0,
      10.0,
      -16.7,
      80.5,
      30.25,
      0.8,
      9.5,
      10.0,
      -2.1
    ]
  ]
}
http_res = requests.post(
  svc.scoring_uri,
  json.dumps(body),
  headers = headers)
print('Result : ', http_res.text)