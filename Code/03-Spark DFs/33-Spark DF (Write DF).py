# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
# df.show()
df.count()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df = df.groupBy("course","gender").count()

# COMMAND ----------

# overwrite
# append
# ignore
# error

# COMMAND ----------

df.write.mode("overwrite").options(header='True').csv('/FileStore/tables/StudentData/output')

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData/output')
df.show()
