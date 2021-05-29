# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import DoubleType
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/OfficeData.csv')
df.show()

# COMMAND ----------

def get_incr(state, salary, bonus):
  sum = 0
  if state == "NY":
    sum = salary * 0.10
    sum += bonus * 0.05
  elif state == "CA":
    sum = salary * 0.12
    sum += bonus * 0.03
  return sum

incrUDF = udf(lambda x,y,z: get_incr(x,y,z), DoubleType())

df.withColumn("increment", incrUDF(df.state, df.salary, df.bonus)).show()

# COMMAND ----------

(90000  * 0.10) + (10000 * 0.05)
