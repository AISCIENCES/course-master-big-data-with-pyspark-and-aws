# Databricks notebook source
# dbutils.fs.rm("/FileStore/tables", True)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import col,explode
spark = SparkSession.builder.appName("Collaborative filtering").getOrCreate()

# COMMAND ----------

moviesDF = spark.read.options(header="True", inferSchema="True").csv("/FileStore/tables/movies.csv")
ratingsDF = spark.read.options(header="True", inferSchema="True").csv("/FileStore/tables/ratings.csv")

# COMMAND ----------

display(moviesDF)

# COMMAND ----------

display(ratingsDF)

# COMMAND ----------

ratings = ratingsDF.join(moviesDF, 'movieId', 'left')

# COMMAND ----------

(train, test) = ratings.randomSplit([0.8,0.2])

# COMMAND ----------

ratings.count()

# COMMAND ----------

print(train.count())
train.show()

# COMMAND ----------

print(test.count())
test.show()

# COMMAND ----------

als = ALS(userCol = "userId", itemCol="movieId", ratingCol="rating", nonnegative=True,implicitPrefs=False, coldStartStrategy="drop")

# COMMAND ----------

param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 50, 100, 150]) \
            .addGrid(als.regParam, [.01, .05, .1, .15]) \
            .build()

# COMMAND ----------

evaluator = RegressionEvaluator(
           metricName="rmse", 
           labelCol="rating", 
           predictionCol="prediction")

# COMMAND ----------

cv = CrossValidator(estimator=als, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=5)

# COMMAND ----------

model = cv.fit(train)
best_model = model.bestModel
test_predictions = best_model.transform(test)
RMSE = evaluator.evaluate(test_predictions)
print(RMSE)

# COMMAND ----------

print(RMSE)

# COMMAND ----------

recommendations = best_model.recommendForAllUsers(5)

# COMMAND ----------

df = recommendations

# COMMAND ----------

display(df)

# COMMAND ----------

df2 = df.withColumn("movieid_rating", explode("recommendations"))

# COMMAND ----------

display(df2)

# COMMAND ----------

display(df2.select("userId", col("movieid_rating.movieId"), col("movieid_rating.rating")))

# COMMAND ----------


