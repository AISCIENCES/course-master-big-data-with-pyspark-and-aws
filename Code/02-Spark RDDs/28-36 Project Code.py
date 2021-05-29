# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Mini Project")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
headers = rdd.first()
rdd = rdd.filter(lambda x: x!=headers)
rdd = rdd.map(lambda x: x.split(','))

# COMMAND ----------

# 1
rdd.count()

# COMMAND ----------

# 2
rdd2 = rdd
rdd2 = rdd2.map(lambda x: (x[1], int(x[5])))
rdd2 = rdd2.reduceByKey(lambda x,y : x+y)
rdd2.collect()

# COMMAND ----------

#3
rdd3 = rdd
passed = rdd3.filter(lambda x: int(x[5]) > 50).count()
failed = rdd3.filter(lambda x: int(x[5]) <= 50).count()
print(passed,failed)

passed2 = rdd3.filter(lambda x: int(x[5]) > 50).count()
failed2 = rdd.count() - passed2
print(passed2,failed2)

# COMMAND ----------

# 4
rdd4 = rdd
rdd4 = rdd4.map(lambda x: (x[3],1))
rdd4.reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

# 5
rdd5 = rdd
rdd5 = rdd5.map(lambda x: (x[3], int(x[5])))
rdd5.reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

# 6
rdd6 = rdd
rdd6 = rdd6.map(lambda x: (x[3], (int(x[5]), 1) ))
rdd6 = rdd6.reduceByKey( lambda x,y : (x[0] + y[0], x[1] + y[1]))

rdd6.map(lambda x: (x[0], (x[1][0] / x[1][1]))).collect()
rdd6.mapValues(lambda x: (x[0] / x[1])).collect()


# COMMAND ----------

# 7
rdd7 = rdd
rdd7 = rdd7.map(lambda x: (x[3], int(x[5])))
print(rdd7.reduceByKey(lambda x,y: x if x > y else y).collect())
print(rdd7.reduceByKey(lambda x,y: x if x < y else y).collect())

# COMMAND ----------

# 8
rdd8 = rdd
rdd8 = rdd8.map( lambda x: ( x[1] , ( int(x[0]), 1 ) ) )
rdd8 = rdd8.reduceByKey(lambda x,y: ( x[0] + y[0], x[1] + y[1] )  )
rdd8.mapValues(lambda x: x[0]/x[1]).collect()
