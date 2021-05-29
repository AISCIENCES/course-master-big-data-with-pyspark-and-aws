# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Min and Max")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/movie_ratings.csv')
rdd.collect()

# COMMAND ----------

x = 3
y = 4
x if x < y else y

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[0],int(x.split(',')[1])))
rdd2.collect()

# COMMAND ----------

rdd2.reduceByKey(lambda x , y : x if x < y else y).collect()

# COMMAND ----------

rdd2.reduceByKey(lambda x , y : x if x > y else y).collect()
