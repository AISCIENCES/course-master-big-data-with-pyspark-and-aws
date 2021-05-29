# Databricks notebook source
dbutils.fs.rm("/FileStore/tables", True)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Sparking Streaming DF").getOrCreate()
word = spark.readStream.text("/FileStore/tables")
word = word.groupBy("value").count()
# word.writeStream.format("console").outputMode("complete").start()

# COMMAND ----------

display(word)

# COMMAND ----------


