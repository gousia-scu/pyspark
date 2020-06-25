#!/usr/bin/env python
# coding: utf-8

# In[28]:


import pyspark
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from pyspark.sql.functions import current_date, datediff 
import datetime

start=datetime.datetime.now()
# sc = pyspark.SparkContext('local[*]')
sc = SparkContext.getOrCreate()

start_load=datetime.datetime.now()
#Loading movies data as dataframe
schemaMovies = StructType([StructField("movieId", IntegerType(), True),                           StructField("title", StringType(), True),                           StructField("genres", StringType(), True)])
moviesDF = spark.read.csv("movies_large.csv",                          header=False,schema=schemaMovies).repartition(1)

#Loading reviews data as dataframe
schemaReviews = StructType([StructField("userId", IntegerType(), True),                            StructField("movieId", IntegerType(), True),                            StructField("rating", DoubleType(), True),                            StructField("timestamp", StringType(), True)])
reviewsDF = spark.read.csv("reviews_large.csv",                           header=False,schema=schemaReviews).repartition(1)
end_load=datetime.datetime.now()
#Finding movies with highest number of reviews
highestRevDF = reviewsDF.select('movieId')    .groupby(['movieId']).count().orderBy('count', ascending=False).limit(10)

# highestRevDF = highestRevDF.cache()
highestRevResDF = moviesDF.alias("a")    .join(highestRevDF.alias("b"), F.col('a.movieId') == col('b.movieId'))    .select('a.movieId','a.title','b.count')    .orderBy('count', ascending=False)
print('Top 10 movies with highest number of reviews')
highestRevResDF.show()

end_part1=datetime.datetime.now()
#Finding movies with average rating>4 and no of reviews>10
avg4SatrDF = reviewsDF.groupBy("movieId")    .agg(F.mean('rating'), F.count('movieId'))    .select(reviewsDF.movieId,            F.col("avg(rating)").alias("average_rating"),            F.col("count(movieId)").alias("no_of_reviews"))    .filter(F.col("count(movieId)") > 10 )    .filter(F.col("avg(rating)")>4)
# avg4SatrDF = avg4SatrDF.cache()
resultDF = moviesDF.alias("a")    .join(avg4SatrDF.alias("b"), F.col('a.movieId') == col('b.movieId'))    .select('a.movieId','a.title','b.average_rating','b.no_of_reviews').orderBy('no_of_reviews', ascending=False)
print('Movies with average rating greather than 4 and number of reviews greater than 10')
resultDF.show()
end_part2=datetime.datetime.now()
load_time=end_load-start_load
print("load_time", load_time.total_seconds())
total_time=(end_part2-start)
print("total time", total_time.total_seconds())
part1= (end_part1-end_load)
print("part1", part1.total_seconds())
part2=(end_part2-end_part1)
print("part2", part2.total_seconds())


# In[ ]:




