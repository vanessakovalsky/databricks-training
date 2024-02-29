# Databricks notebook source
# MAGIC %md
# MAGIC #DataFrame Column Class
# MAGIC
# MAGIC ** Data Source **
# MAGIC * One hour of Pagecounts from the English Wikimedia projects captured August 5, 2016, at 12:00 PM UTC.
# MAGIC * Size on Disk: ~23 MB
# MAGIC * Type: Compressed Parquet File
# MAGIC * More Info: <a href="https://dumps.wikimedia.org/other/pagecounts-raw" target="_blank">Page view statistics for Wikimedia projects</a>
# MAGIC
# MAGIC **Technical Accomplishments:**
# MAGIC * Continue exploring the `DataFrame` set of APIs.
# MAGIC * Introduce the `Column` class

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) **The Data Source**
# MAGIC
# MAGIC We will be using the same data source as our previous notebook.
# MAGIC
# MAGIC As such, we can go ahead and start by creating our initial `DataFrame`.

# COMMAND ----------

(source, sasEntity, sasToken) = getAzureDataSource()
spark.conf.set(sasEntity, sasToken)

parquetFile = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

# COMMAND ----------

pagecountsEnAllDF = (spark  # Our SparkSession & Entry Point
  .read                     # Our DataFrameReader
  .parquet(parquetFile)     # Returns an instance of DataFrame
  .cache()                  # cache the data
)
print(pagecountsEnAllDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take another look at the number of records in our `DataFrame`

# COMMAND ----------

total = pagecountsEnAllDF.count()

print("Record Count: {0:,}".format( total ))

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's take another peek at our data...

# COMMAND ----------

display(pagecountsEnAllDF)

# COMMAND ----------

# MAGIC %md
# MAGIC As we view the data, we can see that there is no real rhyme or reason as to how the data is sorted.
# MAGIC * We cannot even tell if the column **project** is sorted - we are seeing only the first 1,000 of some 2.3 million records.
# MAGIC * The column **article** is not sorted as evident by the article **A_Little_Boy_Lost** appearing between a bunch of articles starting with numbers and symbols.
# MAGIC * The column **requests** is clearly not sorted.
# MAGIC * And our **bytes_served** contains nothing but zeros.
# MAGIC
# MAGIC So let's start by sorting our data. In doing this, we can answer the following question:
# MAGIC
# MAGIC What are the top 10 most requested articles?

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) orderBy(..) & sort(..)
# MAGIC
# MAGIC If you look at the API docs, `orderBy(..)` is described like this:
# MAGIC > Returns a new Dataset sorted by the given expressions.
# MAGIC
# MAGIC Both `orderBy(..)` and `sort(..)` arrange all the records in the `DataFrame` as specified.
# MAGIC * Like `distinct()` and `dropDuplicates()`, `sort(..)` and `orderBy(..)` are aliases for each other.
# MAGIC   * `sort(..)` appealing to functional programmers.
# MAGIC   * `orderBy(..)` appealing to developers with an SQL background.
# MAGIC * Like `orderBy(..)` there are two variants of these two methods:
# MAGIC   * `orderBy(Column)`
# MAGIC   * `orderBy(String)`
# MAGIC   * `sort(Column)`
# MAGIC   * `sort(String)`
# MAGIC
# MAGIC All we need to do now is sort our previous `DataFrame`.

# COMMAND ----------

sortedDF = (pagecountsEnAllDF
  .orderBy("requests")
)
sortedDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see, we are not sorting correctly.
# MAGIC
# MAGIC We need to reverse the sort.
# MAGIC
# MAGIC One might conclude that we could make a call like this:
# MAGIC
# MAGIC `pagecountsEnAllDF.orderBy("requests desc")`
# MAGIC
# MAGIC Try it in the cell below:

# COMMAND ----------

# Uncomment and try this:
# pagecountsEnAllDF.orderBy("requests desc")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Why does this not work?
# MAGIC * The `DataFrames` API is built upon an SQL engine.
# MAGIC * There is a lot of familiarity with this API and SQL syntax in general.
# MAGIC * The problem is that `orderBy(..)` expects the name of the column.
# MAGIC * What we specified was an SQL expression in the form of **requests desc**.
# MAGIC * What we need is a way to programmatically express such an expression.
# MAGIC * This leads us to the second variant, `orderBy(Column)` and more specifically, the class `Column`.
# MAGIC
# MAGIC ** *Note:* ** *Some of the calls in the `DataFrames` API actually accept SQL expressions.*<br/>
# MAGIC *While these functions will appear in the docs as `someFunc(String)` it's very*<br>
# MAGIC *important to thoroughly read and understand what the parameter actually represents.*

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Column Class
# MAGIC
# MAGIC The `Column` class is an object that encompasses more than just the name of the column, but also column-level-transformations, such as sorting in a descending order.
# MAGIC
# MAGIC The first question to ask is how do I create a `Column` object?
# MAGIC
# MAGIC In Scala we have these options:

# COMMAND ----------

# MAGIC %md
# MAGIC ** *Note:* ** *We are showing both the Scala and Python versions below for comparison.*<br/>
# MAGIC *Make sure to run only the one cell for your notebook's default language (Scala or Python)*

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // Scala & Python both support accessing a column from a known DataFrame
# MAGIC // Uncomment this if you are using the Scala version of this notebook
# MAGIC // val columnA = pagecountsEnAllDF("requests")    
# MAGIC
# MAGIC // This option is Scala specific, but is arugably the cleanest and easy to read.
# MAGIC val columnB = $"requests"          
# MAGIC
# MAGIC // If we import ...sql.functions, we get a couple of more options:
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC // This uses the col(..) function
# MAGIC val columnC = col("requests")
# MAGIC
# MAGIC // This uses the expr(..) function which parses an SQL Expression
# MAGIC val columnD = expr("a + 1")
# MAGIC
# MAGIC // This uses the lit(..) to create a literal (constant) value.
# MAGIC val columnE = lit("abc")

# COMMAND ----------

# MAGIC %md
# MAGIC In Python we have these options:

# COMMAND ----------


# Scala & Python both support accessing a column from a known DataFrame
# Uncomment this if you are using the Python version of this notebook
# columnA = pagecountsEnAllDF["requests"]

# The $"column-name" version that works for Scala does not work in Python
# columnB = $"requests"      

# If we import ...sql.functions, we get a couple of more options:
from pyspark.sql.functions import *

# This uses the col(..) function
columnC = col("requests")

# This uses the expr(..) function which parses an SQL Expression
columnD = expr("a + 1")

# This uses the lit(..) to create a literal (constant) value.
columnE = lit("abc")

# Print the type of each attribute
print("columnC: {}".format(columnC))
print("columnD: {}".format(columnD))
print("columnE: {}".format(columnE))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In the case of Scala, the cleanest version is the **$"column-name"** variant.
# MAGIC
# MAGIC In the case of Python, the cleanest version is the **col("column-name")** variant.
# MAGIC
# MAGIC So with that, we can now create a `Column` object, and apply the `desc()` operation to it:
# MAGIC
# MAGIC ** *Note:* ** *We are introducing `...sql.functions` specifically for creating `Column` objects.*<br/>
# MAGIC *We will be reviewing the multitude of other commands available from this part of the API in future notebooks.*

# COMMAND ----------

column = col("requests").desc()

# Print the column type
print("column:", column)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC And now we can piece it all together...

# COMMAND ----------

sortedDescDF = (pagecountsEnAllDF
  .orderBy( col("requests").desc() )
)  
sortedDescDF.show(10, False) # The top 10 is good enough for now

# COMMAND ----------

# MAGIC %md
# MAGIC It should be of no surprise that the **Main_Page** (in both the Wikipedia and Wikimedia projects) is the most requested page.
# MAGIC
# MAGIC Followed shortly after that is **Special:Search**, Wikipedia's search page.
# MAGIC
# MAGIC And if you consider that this data was captured in the August before the 2016 presidential election, the Trumps will be one of the most requested pages on Wikipedia.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review Column Class
# MAGIC
# MAGIC The `Column` objects provide us a programmatic way to build up SQL-ish expressions.
# MAGIC
# MAGIC Besides the `Column.desc()` operation we used above, we have a number of other operations that can be performed on a `Column` object.
# MAGIC
# MAGIC Here is a preview of the various functions - we will cover many of these as we progress through the class:
# MAGIC
# MAGIC **Column Functions**
# MAGIC * Various mathematical functions such as add, subtract, multiply & divide
# MAGIC * Various bitwise operators such as AND, OR & XOR
# MAGIC * Various null tests such as `isNull()`, `isNotNull()` & `isNaN()`.
# MAGIC * `as(..)`, `alias(..)` & `name(..)` - Returns this column aliased with a new name or names (in the case of expressions that return more than one column, such as explode).
# MAGIC * `between(..)` - A boolean expression that is evaluated to true if the value of this expression is between the given columns.
# MAGIC * `cast(..)` & `astype(..)` - Convert the column into type dataType.
# MAGIC * `asc(..)` - Returns a sort expression based on the ascending order of the given column name.
# MAGIC * `desc(..)` - Returns a sort expression based on the descending order of the given column name.
# MAGIC * `startswith(..)` - String starts with.
# MAGIC * `endswith(..)` - String ends with another string literal.
# MAGIC * `isin(..)` - A boolean expression that is evaluated to true if the value of this expression is contained by the evaluated values of the arguments.
# MAGIC * `like(..)` - SQL like expression
# MAGIC * `rlike(..)` - SQL RLIKE expression (LIKE with Regex).
# MAGIC * `substr(..)` - An expression that returns a substring.
# MAGIC * `when(..)` & `otherwise(..)` - Evaluates a list of conditions and returns one of multiple possible result expressions.
# MAGIC
# MAGIC The complete list of functions differs from language to language.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC Start the next lesson, [Work with Column expressions]($./2.DataFrame-Column-Expressions)