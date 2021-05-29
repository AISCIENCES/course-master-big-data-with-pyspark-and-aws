# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Quiz")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample_words.txt')

# COMMAND ----------

flatMappedRdd = rdd.flatMap(lambda x: x.split(' '))

# COMMAND ----------

def filterAandC(x):
  if x.startswith('a') or x.startswith('c'):
    return False
  else:
    return True
  
filteredRdd = flatMappedRdd.filter(filterAandC)

# COMMAND ----------

filteredRdd.collect()

# COMMAND ----------

filteredRddLambda = flatMappedRdd.filter(lambda x: not (x.startswith('a') or x.startswith('c')) )

# COMMAND ----------

filteredRddLambda.collect()

# COMMAND ----------


