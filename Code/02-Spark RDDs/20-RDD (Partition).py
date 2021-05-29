# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Partition")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample_words2.txt')
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))


# COMMAND ----------

rdd3.saveAsTextFile('/FileStore/tables/output/5partitionOutput')

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/sample_words2.txt')
rdd = rdd.repartition(5)
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,1))
rdd3 = rdd3.coalesce(3)

# COMMAND ----------

rdd3.saveAsTextFile('/FileStore/tables/output/3partitionOutput')

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/output/5partitionOutput')
rdd.collect()
