# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------


df = df.withColumn("roll", col("roll").cast("String"))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("marks", col('marks') + 10)
df.show()

# COMMAND ----------

df = df.withColumn("aggregated marks", col('marks') - 10)
df.show()

# COMMAND ----------

df = df.withColumn("name", lit("USA"))
df.show()

# COMMAND ----------

df.show()

# COMMAND ----------

df = df.withColumn("marks", col("marks") - 10).withColumn("updated marks", col("marks") + 20).withColumn("Country", lit("USA"))

# COMMAND ----------

df.show()
