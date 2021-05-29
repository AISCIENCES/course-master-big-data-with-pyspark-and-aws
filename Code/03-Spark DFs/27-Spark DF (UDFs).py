# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import IntegerType
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')
df.show()

# COMMAND ----------

def get_total_salary(salary):
  return salary + 100


totalSalaryUDF = udf(lambda x: get_total_salary(x), IntegerType())




df.withColumn("total_salary", totalSalaryUDF(df.salary)).show()
