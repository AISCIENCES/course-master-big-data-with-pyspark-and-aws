# Databricks notebook source
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Streaming")
sc = SparkContext.getOrCreate(conf=conf)

ssc = StreamingContext(sc, 1)

# COMMAND ----------

rdd = ssc.textFileStream("/FileStore/tables/")

# COMMAND ----------

rdd = rdd.map(lambda x: (x,1))
rdd = rdd.reduceByKey(lambda x,y : x+y)
rdd.pprint()
ssc.start()
ssc.awaitTerminationOrTimeout(1000000)

# COMMAND ----------


