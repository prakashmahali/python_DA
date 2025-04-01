import subprocess
import pandas as pd
import io
import json

# 1️⃣ Execute API Query (Curl) and Convert to DataFrame
username = "your_username"
password = "your_password"
url = "https://your_url"

# Curl command
cmd_api = 'curl -k -u "{}":"{}" {} -d search="|inputlookup gsam_mtn.csv" -d output_mode=csv -d count=0 -d offset=25000'.format(username, password, url)

# Execute command
process_api = subprocess.Popen(cmd_api, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
stdout_api, stderr_api = process_api.communicate()

# Check for errors
if process_api.returncode != 0:
    print("API Error:", stderr_api)
    df_api = None
else:
    df_api = pd.read_csv(io.StringIO(stdout_api))
    print("API DataFrame:\n", df_api.head())


# 2️⃣ Execute BQ Query and Convert to DataFrame
bq_query = "SELECT * FROM `your_project.dataset.your_table` LIMIT 1000"

# BQ command to fetch JSON data
cmd_bq = "bq query --use_legacy_sql=false --format=json '{}'".format(bq_query)

# Execute command
process_bq = subprocess.Popen(cmd_bq, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
stdout_bq, stderr_bq = process_bq.communicate()

# Check for errors
if process_bq.returncode != 0:
    print("BQ Error:", stderr_bq)
    df_bq = None
else:
    bq_data = json.loads(stdout_bq)  # Parse JSON output
    df_bq = pd.DataFrame(bq_data)
    print("BQ DataFrame:\n", df_bq.head())


# 3️⃣ Join API DataFrame with BQ DataFrame
if df_api is not None and df_bq is not None:
    df_merged = pd.merge(df_api, df_bq, how="inner", on="common_column")  # Change "common_column" to actual join column
    print("Merged DataFrame:\n", df_merged.head())
else:
    print("Could not merge data due to errors in fetching API or BQ data.")
