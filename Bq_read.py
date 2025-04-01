import subprocess
import pandas as pd
import json

# Define the BigQuery SQL file
bq_sql_file = "bq.sql"

# Check if the file exists
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

# Debugging: Print errors if return code is non-zero
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
