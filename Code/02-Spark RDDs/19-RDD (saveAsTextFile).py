# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("saveAsTextFile")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample_words2.txt')
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))

# COMMAND ----------

rdd3.saveAsTextFile('/FileStore/tables/ouput/sample_words_output2')

# COMMAND ----------

rdd.getNumPartitions()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

rdd.saveAsTextFile('/FileStore/tables/ouput/sample_words_output.txt')
