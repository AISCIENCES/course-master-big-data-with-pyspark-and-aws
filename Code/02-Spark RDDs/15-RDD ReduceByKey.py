# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("distinct")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))

# COMMAND ----------

rdd3 = rdd2.map(lambda x: (x,1))
rdd3.collect()

# COMMAND ----------

rdd3.reduceByKey(lambda x,y: x+y).collect()
