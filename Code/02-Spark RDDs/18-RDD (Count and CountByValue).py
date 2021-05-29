# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Count")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample_words2.txt')

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))

# COMMAND ----------

rdd2.count()

# COMMAND ----------

rdd.countByValue()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd2.collect()

# COMMAND ----------

rdd2.countByValue()
