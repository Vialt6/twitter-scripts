import random
from shapely.geometry import Point
import geopandas as gpd

# Load the USA boundary shapefile
usa_shapefile = "D://uni//Magistrale//Sistemi distribuiti//shape//shape2//States_shapefile.shp"  # Replace with the actual path to your shapefile

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
            return latitude, longitude

for i in range(30):
    # Example usage
    random_latitude, random_longitude = generate_random_coordinates()
    print("Random USA Coordinates:")
    print("Latitude:", random_latitude)
    print("Longitude:", random_longitude)