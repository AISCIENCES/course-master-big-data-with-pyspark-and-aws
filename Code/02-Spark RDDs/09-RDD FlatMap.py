# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("FlatMap")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample.txt')
rdd.collect()

# COMMAND ----------

flatMappedRdd = rdd.flatMap(lambda x: x.split(" "))
flatMappedRdd.collect()
