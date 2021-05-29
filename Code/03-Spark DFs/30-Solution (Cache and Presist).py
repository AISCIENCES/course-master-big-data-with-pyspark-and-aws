# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import DoubleType
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
# df.show()

# COMMAND ----------

df = df.groupBy("course","gender","age").count()

# COMMAND ----------

df = df.withColumn("dummy", col("age") * 100)

# COMMAND ----------

df.show()

# COMMAND ----------

df.cache()

# COMMAND ----------

df.show()

# COMMAND ----------


