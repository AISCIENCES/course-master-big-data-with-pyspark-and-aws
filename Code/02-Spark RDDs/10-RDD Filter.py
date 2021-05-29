# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("FlatMap")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample.txt')
rdd.collect()

# COMMAND ----------

def foo(x):
  if x == '12 12 33':
    return False
  else:
    return True

rdd2 = rdd.filter(foo)
rdd2.collect()
