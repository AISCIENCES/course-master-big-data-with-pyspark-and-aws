# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

type(df)

# COMMAND ----------

rdd = df.rdd

# COMMAND ----------

type(rdd)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd.filter(lambda x: x[0] == 28 ).collect()

# COMMAND ----------

rdd.filter(lambda x: x["gender"] == "Male" ).collect()
