# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.select("name","gender").show()

# COMMAND ----------

df.select(df.name, df.email).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("roll"), col("name")).show()

# COMMAND ----------

df.select('*').show()

# COMMAND ----------


df.select(df.columns[2:6]).show()

# COMMAND ----------

df2 = df.select(col("roll"), col("name"))

# COMMAND ----------

df2.show()
