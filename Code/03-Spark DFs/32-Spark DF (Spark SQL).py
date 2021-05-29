# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.createOrReplaceTempView("Student")

# COMMAND ----------

spark.sql("select course, gender, count(*) from Student group by course, gender").show()
df.groupBy("course", "gender").count().show()
