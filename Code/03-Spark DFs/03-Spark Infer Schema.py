# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema='True', header='True', delimiter=',').csv('/FileStore/tables/StudentData.csv')
df.show()
df.printSchema()
