from timezonefinder import TimezoneFinder
import h3

# Initialize the TimezoneFinder instance
tf = TimezoneFinder()

# Function to get timezone from latitude and longitude
def get_timezone_from_latlon(latitude, longitude):
    return tf.timezone_at(lat=latitude, lng=longitude)

# Function to get timezone from an H3 index
def get_timezone_from_h3(h3_index):
    # Convert H3 index to latitude and longitude
    lat, lon = h3.h3_to_geo(h3_index)
    # Use the latitude and longitude to get the timezone
    return get_timezone_from_latlon(lat, lon)

# Example usage
latitude, longitude = 40.7128, -74.0060  # New York City coordinates
h3_index = h3.geo_to_h3(latitude, longitude, 10)  # Example H3 index at resolution 10

# Get timezone from latitude and longitude
timezone_latlon = get_timezone_from_latlon(latitude, longitude)
print("Timezone from latitude and longitude:", timezone_latlon)

# Get timezone from H3 index
timezone_h3 = get_timezone_from_h3(h3_index)
print("Timezone from H3 index:", timezone_h3)
