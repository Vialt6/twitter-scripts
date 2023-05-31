from multiprocessing import Pool
import random
from pyspark.sql.functions import to_timestamp, col, lit, desc
import pyspark
import numpy as np
import pandas as pd 
import geopandas as gpd
from shapely.geometry import Point
from pyspark.sql.types import DoubleType

# Import SparkSession
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

df = spark.read.json("D://uni//Magistrale//Sistemi distribuiti//progetto//data//us-presidential-tweet-id-2020-10-01-04_hydrated.json")
#df.show()

coordinates_df = df.select("coordinates")
#coordinates_df.show(truncate=False)

pandas_df = coordinates_df.toPandas()
print(pandas_df.head())

# Load the USA boundary shapefile
usa_shapefile = "D://uni//Magistrale//Sistemi distribuiti//shape//shape1//USA_States.shp"  # Replace with the actual path to your shapefile

# Read the shapefile into a GeoDataFrame
usa_boundary = gpd.read_file(usa_shapefile)

def generate_random_coordinates():
    # Generate random points within the polygon geometry of the USA boundary
    while True:
        # Select a random polygon from the USA boundary
        random_polygon = random.choice(usa_boundary.geometry)
        
        # Generate random coordinates within the selected polygon
        minx, miny, maxx, maxy = random_polygon.bounds
        latitude = random.uniform(miny, maxy)
        longitude = random.uniform(minx, maxx)
        
        # Create a Point object from the generated coordinates
        point = Point(longitude, latitude)
        
        # Check if the point is within the selected polygon
        if random_polygon.contains(point):
            print(latitude, longitude)
            return latitude, longitude

pandas_df['coordinates'] = pandas_df['coordinates'].apply(lambda x: generate_random_coordinates()
                                                                if pd.isnull(x) else x)



new_df = pd.DataFrame()

# Extract latitude and longitude from the 'coordinates' column
new_df['latitude'] = pandas_df['coordinates'].apply(lambda x: x[0] if pd.notnull(x) else None)
new_df['longitude'] = pandas_df['coordinates'].apply(lambda x: x[1] if pd.notnull(x) else None)

# Print the new DataFrame
print(new_df)

new_df.to_json('D://uni//espython//twitter//data//tweet-shape-coordinates-04.json', orient='records')

