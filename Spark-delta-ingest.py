import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

from datetime import datetime, timedelta, date
from pyspark.sql.functions import udf, expr, concat, col
from delta.tables import *

spark = SparkSession \
     .builder \
     .appName("Python Spark SQL basic example") \
     .enableHiveSupport() \
     .getOrCreate()


brokers = sys.argv[1]
checkpoint_dir = sys.argv[2]
delta_loc = sys.argv[3]


Schema_json = spark.read.option("multiLine", "true").json("/tmp/spark-twitter-realtime-analyzer/tweet.json").schema

spark.sql("CREATE DATABASE IF NOT EXISTS twitter_db")
spark.sql("DROP TABLE IF EXISTS twitter_db.tweets_delta")
spark.sql("CREATE TABLE twitter_db.tweets_delta USING DELTA LOCATION '{0}'".format(delta_loc))

kafkaStreamDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribe", "tweets").option("startingOffsets", 'latest').load().selectExpr("CAST(value AS STRING)")

json_df = kafkaStreamDF.select(from_json(col("value"),Schema_json).alias("values"))

flattenedJsonDf = json_df.select(col("values.created_at").alias("created_at"),col("values.user.screen_name").alias("screen_name"),col("values.user.name").alias("name"),col("values.user.followers_count").alias("followers_count"),col("values.user.statuses_count").alias("statuses_count"),col("values.user.favourites_count").alias("favourites_count"),col("values.user.profile_image_url").alias("profile_image_url"),col("values.user.friends_count").alias("friends_count"),col("values.user.id").alias("id_usr"),col("values.id").alias("id"),col("values.user.location").alias("location"),col("values.text").alias("text"),col("values.source").alias("source"),col("values.in_reply_to_status_id").alias("in_reply_to_status_id"),col("values.in_reply_to_status_id_str").alias("in_reply_to_status_id_str"),col("values.in_reply_to_user_id").alias("in_reply_to_user_id"),col("values.in_reply_to_user_id_str").alias("in_reply_to_user_id_str"),col("values.in_reply_to_screen_name").alias("in_reply_to_screen_name"),col("values.geo").alias("geo"),col("values.coordinates").alias("coordinates"),col("values.place").alias("place"),col("values.favorite_count").alias("favorite_count"),col("values.lang").alias("lang"),col("values.entities.hashtags.text").alias("hashtags"))

query = flattenedJsonDf.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_dir).start(delta_loc)

query.awaitTermination()
