import subprocess
import pandas as pd
import json
import io
from pymongo import MongoClient

# MongoDB Connection
mongo_client = MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB connection string
db = mongo_client['your_database']  # Replace with your database name
collection = db['your_collection']  # Replace with your collection name

# 1️⃣ Execute API Query (Curl) and Convert to DataFrame
username = "your_username"
password = "your_password"
url = "https://your_url"

# Curl command
cmd_api = 'curl -k -u "{}":"{}" {} -d search="|inputlookup gsam_mtn.csv" -d output_mode=csv -d count=0 -d offset=25000'.format(username, password, url)

# Execute command
process_api = subprocess.Popen(cmd_api, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
stdout_api, stderr_api = process_api.communicate()

# Check for errors in API call
if process_api.returncode != 0:
    print("API Error:", stderr_api)
    df_api = None
else:
    df_api = pd.read_csv(io.StringIO(stdout_api))
    print("API DataFrame:\n", df_api.head())


# 2️⃣ Execute the BQ Query from File and Convert to DataFrame
bq_sql_file = "bq.sql"

# Read the SQL file
try:
    with open(bq_sql_file, "r") as file:
        bq_query = file.read().strip()
except FileNotFoundError:
    print("Error: bq.sql file not found!")
    exit(1)

# BQ command to fetch JSON data
cmd_bq = "bq query --nouse_legacy_sql --format=json '{}'".format(bq_query)

# Execute command
process_bq = subprocess.Popen(cmd_bq, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
stdout_bq, stderr_bq = process_bq.communicate()

# Check for errors in BQ query
if process_bq.returncode != 0:
    print("BQ Error:\n", stderr_bq)
    exit(1)

# Convert JSON output to DataFrame
try:
    bq_data = json.loads(stdout_bq)  # Parse JSON output
    df_bq = pd.DataFrame(bq_data)
    print("BigQuery DataFrame:\n", df_bq.head())
except json.JSONDecodeError as e:
    print("Error parsing JSON output:", e)
    print("Raw Output:\n", stdout_bq)
    exit(1)


# 3️⃣ Perform LEFT JOIN on DataFrames
if df_api is not None and df_bq is not None:
    # Specify the column name to join on (e.g., 'common_column')
    df_merged = pd.merge(df_api, df_bq, how="left", on="common_column")  # Change 'common_column' to actual column name
    print("Merged DataFrame:\n", df_merged.head())

    # Convert the merged DataFrame to JSON format
    json_output = df_merged.to_json(orient="records", indent=4)
    
    # 4️⃣ Write the merged data to MongoDB collection
    # Convert the JSON string to a Python object (list of dictionaries)
    json_data = json.loads(json_output)

    # Insert data into MongoDB collection
    collection.insert_many(json_data)  # Insert multiple records

    print("Data inserted into MongoDB successfully!")

else:
    print("Could not merge data due to errors in fetching API or BQ data.")
