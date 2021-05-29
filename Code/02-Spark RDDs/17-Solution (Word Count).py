# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Quiz")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample_words2.txt')

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(' '))

# COMMAND ----------

rdd3 = rdd2.filter(lambda x: len(x) != 0)

# COMMAND ----------

rdd4 = rdd3.map(lambda x: (x,1))

# COMMAND ----------

rdd4.reduceByKey(lambda x,y : x+y).collect()

# COMMAND ----------

rdd.flatMap(lambda x : x.split(' ')).map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y).collect()

# COMMAND ----------


