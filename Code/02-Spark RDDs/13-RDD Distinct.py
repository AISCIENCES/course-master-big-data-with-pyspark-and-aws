# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("distinct")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample.txt')

# COMMAND ----------

rdd2 = rdd.distinct()
rdd2.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.distinct()
rdd3.collect()

# COMMAND ----------

rdd.flatMap(lambda x: x.split(" ")).distinct().collect()
