# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName("Read File")

# COMMAND ----------

sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

text = sc.textFile('/FileStore/tables/sample.txt')

# COMMAND ----------

print('\n\n\n')
print(text.collect())
print('\n\n\n')

