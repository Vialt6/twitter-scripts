from pyspark.sql.functions import to_timestamp, col, lit, desc
import pyspark
import numpy as np
import pandas as pd 


# Import SparkSession
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

df = spark.read.json("D://uni//Magistrale//Sistemi distribuiti//progetto//data//us-presidential-tweet-id-2020-10-01-04_hydrated.json")
df_geo = spark.read.json("D://uni//espython//twitter//data//tweet-shape-coordinates-04.JSON")




selected_df = df.select(
    col("user.id").alias("id"),
    col("user.name").alias("name"),
    col("user.screen_name").alias("screen_name"),
    col("full_text").alias("full_text"),
    col("created_at").alias("created_at"),
    col("retweet_count").alias("retweet_count"),
    col("favorite_count").alias("favorite_count")
)
selected_df_pd = selected_df.toPandas()
df_geo_pd = df_geo.toPandas()
final_df = selected_df_pd.merge(df_geo_pd, left_index=True, right_index=True)
print(final_df)
final_df.to_json('D://uni//espython//twitter//data//tweet-final-shape-04.json', orient='records')