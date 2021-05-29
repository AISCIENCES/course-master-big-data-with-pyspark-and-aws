# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df = df.withColumn("total_marks", lit(120))

# COMMAND ----------

df = df.withColumn("average", (col("marks") / col ("total_marks"))*100 )

# COMMAND ----------

df_OOP = df.filter((df.course == "OOP") & (df.average > 80))

# COMMAND ----------

df_Cloud = df.filter((df.course == "Cloud") & (df.average > 60))

# COMMAND ----------

df_OOP.select("name","marks").show()

# COMMAND ----------

df_Cloud.select("name","marks").show()
