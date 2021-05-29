# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("distinct")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample_words.txt')

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))

# COMMAND ----------

rdd3 = rdd2.map(lambda x: (x,len(x)))
rdd3.collect()

# COMMAND ----------

rdd3.groupByKey().mapValues(list).collect()

# COMMAND ----------


