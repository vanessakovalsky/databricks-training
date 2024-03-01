-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks SQL in 5 minutes
-- MAGIC Learn how to spin up a cluster, download data using `shell`, run SQL queries on that data, and display results in 5 minutes.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a quickstart cluster
-- MAGIC
-- MAGIC 1. In the sidebar, right-click the **Clusters** button and open the link in a new window.
-- MAGIC 1. On the Clusters page, click **Create Cluster**.
-- MAGIC 1. Name the cluster **Cluster Quickstart**.
-- MAGIC 1. In the Databricks Runtime Version drop-down, select **7.x (Scala 2.12, Spark 3.0.1)**.
-- MAGIC 1. Click **Create Cluster**.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Import this notebook
-- MAGIC
-- MAGIC To import this notebook:
-- MAGIC * From your browser, go to the open the [Databricks SQL Quick Start notebook](https://github.com/databricks/tech-talks/blob/master/samples/Databricks%20SQL%20Quick%20Start.dbc) in the Databricks TechTalks github repo.
-- MAGIC * Copy the URL of the notebook or you can do it directly from here: `https://github.com/databricks/tech-talks/blob/master/samples/Databricks%20SQL%20Quick%20Start.dbc`
-- MAGIC * [Import the notebook](https://docs.databricks.com/notebooks/notebooks-manage.html#import-a-notebook) as noted in the following flow.
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/Import%20Notebook.gif" width=800/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Attach the notebook to the cluster and run all commands in the notebook
-- MAGIC
-- MAGIC 1. Return to this notebook. 
-- MAGIC 1. In the notebook menu bar, select **<img src="http://docs.databricks.com/_static/images/notebooks/detached.png"/></a> > Cluster Quickstart**.
-- MAGIC 1. When the cluster changes from <img src="http://docs.databricks.com/_static/images/clusters/cluster-starting.png"/></a> to <img src="http://docs.databricks.com/_static/images/clusters/cluster-running.png"/></a>, you're ready to start!
-- MAGIC 1. To run each cell in the notebook, go to each cell and type `<shift>` + `<enter>` or click **<img src="http://docs.databricks.com/_static/images/notebooks/run-all.png"/></a> Run All** to run all of the cells in the notebook.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Download a data source file to your cluster
-- MAGIC There are many mechanisms to [import data into Databricks](https://docs.databricks.com/data/data.html). The following example uses a [shell command](https://docs.databricks.com/notebooks/notebooks-use.html) in this notebook:
-- MAGIC * Creates a temporary directory on [DBFS](https://docs.databricks.com/data/databricks-file-system.html)
-- MAGIC * Downloads the  `data_geo.csv` file which contains 2014 City Population Estimates vs. 2015 Median Home Sales
-- MAGIC * Shows up the file using `ls -al`

-- COMMAND ----------

-- MAGIC %sh mkdir -p /dbfs/tmp/samples/ && wget -O /dbfs/tmp/samples/data_geo.csv https://raw.githubusercontent.com/databricks/tech-talks/master/datasets/data_geo.csv && ls -al /dbfs/tmp/samples/

-- COMMAND ----------

-- MAGIC %md ## Create `data_geo` DataFrame
-- MAGIC There are multiple routes to [import data into Databricks](https://docs.databricks.com/data/index.html).  As we have already downloaded the file to our cluster, our two simplest options are:
-- MAGIC 1. Import the data via `pandas` and convert the `pandas` DataFrame into a `spark` DataFrame
-- MAGIC 1. Create a `spark` DataFrame directly (this code is commented for now)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Import CSV as pandas 
-- MAGIC import pandas as pd
-- MAGIC pdf_data_geo = pd.read_csv("/dbfs/tmp/samples/data_geo.csv")
-- MAGIC
-- MAGIC # Convert to Spark DataFrame
-- MAGIC data_geo = spark.createDataFrame(pdf_data_geo)
-- MAGIC
-- MAGIC # Create temporary view of data_geo DataFrame
-- MAGIC data_geo.createOrReplaceTempView("data_geo")

-- COMMAND ----------

-- %python
-- # Create data_geo Spark DataFrame
-- data_geo = spark.read \
--    .option("header", "true")\
--    .option("inferSchema", "true")\
--    .csv("/tmp/samples/data_geo.csv")

-- COMMAND ----------

-- MAGIC %md ### View Maximum Median Home Sale Price by State Map
-- MAGIC * Run the following SQL statement 
-- MAGIC * By default, the value is a table.  
-- MAGIC * To view it as a map, click the ![](https://docs.databricks.com/_images/chart-button.png) button and choose the map visualization (globe icon)
-- MAGIC * Hover over the state to see the median price

-- COMMAND ----------

SELECT `State Code`, MAX(`2015 median sales price`) AS 2015_max_median_sales_price
  FROM data_geo
 GROUP BY `State Code`

-- COMMAND ----------

-- MAGIC %md ## View Top 10 Cities by 2015 Median Sales Price and 2014 Home Sales Price
-- MAGIC
-- MAGIC * Run the following SQL statement
-- MAGIC * By default, the value is a table.
-- MAGIC * To view it as a map, click the ![](https://docs.databricks.com/_images/chart-button.png) button and choose the pie chart visualization (pie chart icon)
-- MAGIC * Click **Plot Options...** and ensure that:
-- MAGIC   * Keys: Cities
-- MAGIC   * Values: `2014 Population Estimate (1000s)`, `2015 Median Sales Price (1000s)`

-- COMMAND ----------

SELECT 
 City
,`2014 Population estimate`/1000 AS `2014 Population Estimate (1000s)`
,`2015 median sales price` AS `2015 Median Sales Price (1000s)`
FROM data_geo 
ORDER BY `2015 median sales price` DESC 
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md ### View Population Estimates and Median Sales Price for Washington State cities
-- MAGIC * Run the following SQL statement to view the table

-- COMMAND ----------

SELECT City, `2014 Population estimate`, `2015 median sales price` 
  FROM data_geo 
 WHERE `State Code` = 'WA';

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ## 2015 Max Median Sales Price by State
-- MAGIC
-- MAGIC Let's list out the states that have more than one city per state with non-NULL home prices 
-- MAGIC * To view it as a bar chart, click the ![](https://docs.databricks.com/_images/chart-button.png) button and choose the bar chart visualization 

-- COMMAND ----------

SELECT `State Code`, median_sales_price_max 
  FROM (
    SELECT `State Code`, MAX(`2015 median sales price`) AS median_sales_price_max 
      FROM data_geo 
     WHERE `2015 median sales price` IS NOT NULL
     GROUP BY `State Code`
    HAVING COUNT(1) > 1
  ) a
 ORDER BY median_sales_price_max DESC

-- COMMAND ----------

-- MAGIC %md #### How do I create a temporary table with the just the above cities?
-- MAGIC * Create the `top_median_sales` DataFrame using the following Python code
-- MAGIC * Note that it uses the same SQL statement as in the previous cell
-- MAGIC * Then use the `.createOrReplaceTempView` method to create the temporary view

-- COMMAND ----------

-- MAGIC %python
-- MAGIC top_median_sales = sql("""
-- MAGIC SELECT `State Code`, median_sales_price_max 
-- MAGIC   FROM (
-- MAGIC     SELECT `State Code`, MAX(`2015 median sales price`) AS median_sales_price_max 
-- MAGIC       FROM data_geo 
-- MAGIC      WHERE `2015 median sales price` IS NOT NULL
-- MAGIC      GROUP BY `State Code`
-- MAGIC     HAVING COUNT(1) > 1     
-- MAGIC   ) a
-- MAGIC  ORDER BY median_sales_price_max DESC""")
-- MAGIC top_median_sales.createOrReplaceTempView("top_median_sales")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### 2015 Median Sales Price Box Plot
-- MAGIC Let's show a box plot for the `top_median_cities` to show the variation of prices
-- MAGIC * Run the following SQL statement
-- MAGIC * Click the ![](https://docs.databricks.com/_images/chart-button.png) button and choose the box plot visualization 

-- COMMAND ----------

SELECT f.`State Code`, f.`2015 median sales price`
  FROM data_geo f
    JOIN top_median_sales t
      ON t.`State Code` = f.`State Code`
 WHERE `2015 median sales price` IS NOT NULL
 ORDER BY `2015 median sales price` DESC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## 2015 Median Sales Price by State Histogram
-- MAGIC Let's review the median sales price as a histogram
-- MAGIC * Run the following SQL statement
-- MAGIC * Click the ![](https://docs.databricks.com/_images/chart-button.png) button and choose the histogram visualization 
-- MAGIC * Click **Plot Options...** and ensure that:
-- MAGIC   * Keys: State Code
-- MAGIC   * Values: `2015 Median Sales Price (1000s)`
-- MAGIC   * Number of Bins: 5
-- MAGIC   
-- MAGIC A cursory review of this data seems to indicate a dropping point of around 200K

-- COMMAND ----------

select f.`State Code`, f.`2015 median sales price` 
from data_geo f
    JOIN top_median_sales t
      ON t.`State Code` = f.`State Code`
order by f.`2015 median sales price` desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 2015 Median Sales Price Q-Q Plot
-- MAGIC
-- MAGIC In estimating the quantiles to be plotted, notice the difference in slopes for different states especially when comparing between <200K and >=200K home sales price. 
-- MAGIC
-- MAGIC * Run the following SQL statement
-- MAGIC * Click the ![](https://docs.databricks.com/_images/chart-button.png) button and choose the Q-Q plot visualization 
-- MAGIC * Click **Plot Options...** and ensure that:
-- MAGIC   * Keys: `Category`, `State Code`
-- MAGIC   * Values: `2015 Median Sales Price (1000s)`
-- MAGIC
-- MAGIC Try removing and adding `State Code`.
-- MAGIC
-- MAGIC References:
-- MAGIC * Quantile plots help describe distributions (in this case, the distribution of sales prices across cities) and highlight aspects such as skewed distributions. 
-- MAGIC * Q-Q (Quantile-Quantile) plots provide yet another view of distributions; see Wikipedia on [Q-Q Plots](https://en.wikipedia.org/wiki/Q%E2%80%93Q_plot) for more background.

-- COMMAND ----------

SELECT f.`State Code`, 
       CASE 
          WHEN f.`2015 median sales price` >= 200 THEN '>=200K' 
          WHEN f.`2015 median sales price` < 200 THEN '< 200K' 
       END AS `Category`, 
       `2015 median sales price` 
  FROM data_geo f
    JOIN top_median_sales t
      ON t.`State Code` = f.`State Code`
 WHERE f.`2015 median sales price` IS NOT NULL
 ORDER BY f.`2015 median sales price` DESC

-- COMMAND ----------

