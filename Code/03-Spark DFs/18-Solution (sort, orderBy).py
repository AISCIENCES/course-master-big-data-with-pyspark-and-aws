# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')
df.show()

# COMMAND ----------

df2 = df.sort(df.bonus.asc())
df2.show()

# COMMAND ----------

df3 = df.sort(df.age.desc(), df.salary.asc())
df3.show()

# COMMAND ----------

df4 = df.sort(df.age.desc(), df.bonus.desc(), df.salary.asc())
df4.show()
