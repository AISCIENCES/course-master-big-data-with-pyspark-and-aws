# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Quiz")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/average_quiz_sample.csv')
rdd.collect()

# COMMAND ----------

rdd = rdd.map(lambda x: x.split(','))
rdd = rdd.map(lambda x: (x[1], float(x[2])))

# COMMAND ----------

rdd.reduceByKey(lambda x,y: x if x > y else y).collect()

# COMMAND ----------

rdd.reduceByKey(lambda x,y: x if x < y else y).collect()
