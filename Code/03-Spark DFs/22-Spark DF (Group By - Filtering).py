# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

from pyspark.sql.functions import sum,avg,max,min,mean,count

# COMMAND ----------

df.filter(df.gender == "Male").groupBy("course","gender").agg(count('*').alias("total_enrollments")).filter(col("total_enrollments") > 85).show()

# COMMAND ----------

# Alternate way
df2 = df.filter(df.gender == "Male").groupBy("course","gender").agg(count('*').alias("total_enrollments"))
df2.filter(col("total_enrollments") > 85).show()
