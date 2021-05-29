# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("QUIZ")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Quiz_Sample.txt')
rdd.collect()

# COMMAND ----------

def quiz(x):
  # x -> 'great'
  l = x.split(' ') # l -> ['great']
  l2 = []
  for s in l:
    l2.append(len(s))
  return l2
  
rdd2 = rdd.map(quiz)
rdd2.collect()
