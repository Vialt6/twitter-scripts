import json

# Read the JSON file
with open('D://uni//espython//twitter//data//tweet-final-shape-04.json', 'r') as file:
    data = json.load(file)

# Modify the JSON data as needed
for item in data:
    latitude = item['latitude']
    longitude = item['longitude']
    item['location'] = {
        'lat': latitude,
        'lon': longitude
    }
    del item['latitude']
    del item['longitude']

# Save the modified data to a new JSON file
new_file_path = 'D://uni//espython//twitter//data//tweet-final-shape-04.json'
with open(new_file_path, 'w') as file:
    json.dump(data, file)

print(f"Modified JSON data saved to {new_file_path}")