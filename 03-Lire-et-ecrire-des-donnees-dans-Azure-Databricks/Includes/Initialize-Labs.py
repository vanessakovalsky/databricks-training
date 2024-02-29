# Databricks notebook source
testResults = dict()

def toHash(value):
  from pyspark.sql.functions import hash
  from pyspark.sql.functions import abs
  values = [(value,)]
  return spark.createDataFrame(values, ["value"]).select(abs(hash("value")).cast("int")).first()[0]

def clearYourResults(passedOnly = True):
  whats = list(testResults.keys())
  for what in whats:
    passed = testResults[what][0]
    if passed or passedOnly == False : del testResults[what]

def validateYourSchema(what, df, expColumnName, expColumnType = None):
  label = "{}:{}".format(expColumnName, expColumnType)
  key = "{} contains {}".format(what, label)

  try:
    actualType = df.schema[expColumnName].dataType.typeName()
    
    if expColumnType == None: 
      testResults[key] = (True, "validated")
      print("""{}: validated""".format(key))
    elif actualType == expColumnType:
      testResults[key] = (True, "validated")
      print("""{}: validated""".format(key))
    else:
      answerStr = "{}:{}".format(expColumnName, actualType)
      testResults[key] = (False, answerStr)
      print("""{}: NOT matching ({})""".format(key, answerStr))
  except:
      testResults[what] = (False, "-not found-")
      print("{}: NOT found".format(key))
      
def validateYourAnswer(what, expectedHash, answer):
  # Convert the value to string, remove new lines and carriage returns and then escape quotes
  if (answer == None): answerStr = "null"
  elif (answer is True): answerStr = "true"
  elif (answer is False): answerStr = "false"
  else: answerStr = str(answer)

  hashValue = toHash(answerStr)
  
  if (hashValue == expectedHash):
    testResults[what] = (True, answerStr)
    print("""{} was correct, your answer: {}""".format(what, answerStr))
  else:
    testResults[what] = (False, answerStr)
    print("""{} was NOT correct, your answer: {}""".format(what, answerStr))

def summarizeYourResults():
  html = """<html><body><div style="font-weight:bold; font-size:larger; border-bottom: 1px solid #f0f0f0">Your Answers</div><table style='margin:0'>"""

  whats = list(testResults.keys())
  whats.sort()
  for what in whats:
    passed = testResults[what][0]
    answer = testResults[what][1]
    color = "green" if (passed) else "red" 
    passFail = "passed" if (passed) else "FAILED" 
    html += """<tr style='font-size:larger; white-space:pre'>
                  <td>{}:&nbsp;&nbsp;</td>
                  <td style="color:{}; text-align:center; font-weight:bold">{}</td>
                  <td style="white-space:pre; font-family: monospace">&nbsp;&nbsp;{}</td>
                </tr>""".format(what, color, passFail, answer)
  html += "</table></body></html>"
  displayHTML(html)

def logYourTest(path, name, value):
  value = float(value)
  if "\"" in path: raise ValueError("The name cannot contain quotes.")
  
  dbutils.fs.mkdirs(path)

  csv = """ "{}","{}" """.format(name, value).strip()
  file = "{}/{}.csv".format(path, name).replace(" ", "-").lower()
  dbutils.fs.put(file, csv, True)

def loadYourTestResults(path):
  from pyspark.sql.functions import col
  return spark.read.schema("name string, value double").csv(path)

def loadYourTestMap(path):
  rows = loadYourTestResults(path).collect()
  
  map = dict()
  for row in rows:
    map[row["name"]] = row["value"]
  
  return map

None

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC
# MAGIC val testResults = scala.collection.mutable.Map[String, (Boolean, String)]()
# MAGIC
# MAGIC def toHash(value:String):Int = {
# MAGIC   import org.apache.spark.sql.functions.hash
# MAGIC   import org.apache.spark.sql.functions.abs
# MAGIC   spark.createDataset(List(value)).select(abs(hash($"value")).cast("int")).as[Int].first()
# MAGIC }
# MAGIC
# MAGIC def clearYourResults(passedOnly:Boolean = true):Unit = {
# MAGIC   val whats = testResults.keySet.toSeq.sorted
# MAGIC   for (what <- whats) {
# MAGIC     val passed = testResults(what)._1
# MAGIC     if (passed || passedOnly == false) testResults.remove(what)
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def validateYourSchema(what:String, df:DataFrame, expColumnName:String, expColumnType:String = null):Unit = {
# MAGIC   val label = s"$expColumnName:$expColumnType"
# MAGIC   val key = s"$what contains $label"
# MAGIC   
# MAGIC   try{
# MAGIC     val actualTypeTemp = df.schema(expColumnName).dataType.typeName
# MAGIC     val actualType = if (actualTypeTemp.startsWith("decimal")) "decimal" else actualTypeTemp
# MAGIC     
# MAGIC     if (expColumnType == null) {
# MAGIC       testResults.put(key,(true, "validated"))
# MAGIC       println(s"""$key: validated""")
# MAGIC       
# MAGIC     } else if (actualType == expColumnType) {
# MAGIC       val answerStr = "%s:%s".format(expColumnName, actualType)
# MAGIC       testResults.put(key,(true, "validated"))
# MAGIC       println(s"""$key: validated""")
# MAGIC       
# MAGIC     } else {
# MAGIC       val answerStr = "%s:%s".format(expColumnName, actualType)
# MAGIC       testResults.put(key,(false, answerStr))
# MAGIC       println(s"""$key: NOT matching ($answerStr)""")
# MAGIC     }
# MAGIC   } catch {
# MAGIC     case e:java.lang.IllegalArgumentException => {
# MAGIC       testResults.put(key,(false, "-not found-"))
# MAGIC       println(s"$key: NOT found")
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def validateYourAnswer(what:String, expectedHash:Int, answer:Any):Unit = {
# MAGIC   // Convert the value to string, remove new lines and carriage returns and then escape quotes
# MAGIC   val answerStr = if (answer == null) "null" 
# MAGIC   else answer.toString
# MAGIC
# MAGIC   val hashValue = toHash(answerStr)
# MAGIC
# MAGIC   if (hashValue == expectedHash) {
# MAGIC     testResults.put(what,(true, answerStr))
# MAGIC     println(s"""$what was correct, your answer: ${answerStr}""")
# MAGIC   } else{
# MAGIC     testResults.put(what,(false, answerStr))
# MAGIC     println(s"""$what was NOT correct, your answer: ${answerStr}""")
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def summarizeYourResults():Unit = {
# MAGIC   var html = """<html><body><div style="font-weight:bold; font-size:larger; border-bottom: 1px solid #f0f0f0">Your Answers</div><table style='margin:0'>"""
# MAGIC
# MAGIC   val whats = testResults.keySet.toSeq.sorted
# MAGIC   for (what <- whats) {
# MAGIC     val passed = testResults(what)._1
# MAGIC     val answer = testResults(what)._2
# MAGIC     val color = if (passed) "green" else "red" 
# MAGIC     val passFail = if (passed) "passed" else "FAILED" 
# MAGIC     html += s"""<tr style='font-size:larger; white-space:pre'>
# MAGIC                   <td>${what}:&nbsp;&nbsp;</td>
# MAGIC                   <td style="color:${color}; text-align:center; font-weight:bold">${passFail}</td>
# MAGIC                   <td style="white-space:pre; font-family: monospace">&nbsp;&nbsp;${answer}</td>
# MAGIC                 </tr>"""
# MAGIC   }
# MAGIC   html += "</table></body></html>"
# MAGIC   displayHTML(html)
# MAGIC }
# MAGIC
# MAGIC def logYourTest(path:String, name:String, value:Double):Unit = {
# MAGIC   if (path.contains("\"")) throw new IllegalArgumentException("The name cannot contain quotes.")
# MAGIC   
# MAGIC   dbutils.fs.mkdirs(path)
# MAGIC
# MAGIC   val csv = """ "%s","%s" """.format(name, value).trim()
# MAGIC   val file = "%s/%s.csv".format(path, name).replace(" ", "-").toLowerCase
# MAGIC   dbutils.fs.put(file, csv, true)
# MAGIC }
# MAGIC
# MAGIC def loadYourTestResults(path:String):org.apache.spark.sql.DataFrame = {
# MAGIC   return spark.read.schema("name string, value double").csv(path)
# MAGIC }
# MAGIC
# MAGIC def loadYourTestMap(path:String):scala.collection.mutable.Map[String,Double] = {
# MAGIC   case class TestResult(name:String, value:Double)
# MAGIC   val rows = loadYourTestResults(path).collect()
# MAGIC   
# MAGIC   val map = scala.collection.mutable.Map[String,Double]()
# MAGIC   for (row <- rows) map.put(row.getString(0), row.getDouble(1))
# MAGIC   
# MAGIC   return map
# MAGIC }
# MAGIC
# MAGIC displayHTML("""
# MAGIC   <div>Initializing lab environment:</div>
# MAGIC   <li>Declared <b style="color:green">clearYourResults(<i>passedOnly:Boolean=true</i>)</b></li>
# MAGIC   <li>Declared <b style="color:green">validateYourSchema(<i>what:String, df:DataFrame, expColumnName:String, expColumnType:String</i>)</b></li>
# MAGIC   <li>Declared <b style="color:green">validateYourAnswer(<i>what:String, expectedHash:Int, answer:Any</i>)</b></li>
# MAGIC   <li>Declared <b style="color:green">summarizeYourResults()</b></li>
# MAGIC   <li>Declared <b style="color:green">logYourTest(<i>path:String, name:String, value:Double</i>)</b></li>
# MAGIC   <li>Declared <b style="color:green">loadYourTestResults(<i>path:String</i>)</b> returns <b style="color:green">DataFrame</b></li>
# MAGIC   <li>Declared <b style="color:green">loadYourTestMap(<i>path:String</i>)</b> returns <b style="color:green">Map[String,Double]</b></li>
# MAGIC """)
# MAGIC