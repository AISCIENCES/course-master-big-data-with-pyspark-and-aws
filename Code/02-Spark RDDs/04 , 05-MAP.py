# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Read File")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample.txt')

# COMMAND ----------

rdd2 = rdd.map(lambda x: x.split(' '))
rdd2.collect()

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

def foo(x):
  l = x.split()
  l2 = []
  for s in l:
    l2.append(int(s) + 10)
  return l2

rdd3 = rdd.map(foo)
rdd3.collect()

# COMMAND ----------


