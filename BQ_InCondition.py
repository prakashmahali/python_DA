import h3
import os
from google.cloud import bigquery
from cassandra.cluster import Cluster

# Input H3_6 index
h3_6 = '862a10723ffffff'

# Convert H3_6 to H3_10
h3_10_indices = h3.h3_to_children(h3_6, 10)
h3_10_list = list(h3_10_indices)

# 1. BigQuery Comparison
# Initialize lists to store matching and non-matching H3_10 indices
matching_h3_10_bq = []
non_matching_h3_10_bq = []

# BigQuery table and project information
bq_table = 'your_project.your_dataset.your_table'

# Construct a BigQuery query to check if the H3_10 indices are in the table
h3_10_str = ",".join([f"'{h3_10}'" for h3_10 in h3_10_list])
query_bq = f"SELECT h3_index FROM `{bq_table}` WHERE h3_index IN ({h3_10_str})"

# Run the query using the bq command-line tool
result_bq = os.popen(f"bq query --use_legacy_sql=false \"{query_bq}\"").read()

# Process the BigQuery results
found_h3_10_bq = result_bq.splitlines()

for h3_10 in h3_10_list:
    if h3_10 in found_h3_10_bq:
        matching_h3_10_bq.append(h3_10)
    else:
        non_matching_h3_10_bq.append(h3_10)

# 2. Cassandra Comparison
matching_h3_10_cassandra = []
non_matching_h3_10_cassandra = []

# Setup Cassandra connection
cluster = Cluster(['your_cassandra_host'])
session = cluster.connect('your_keyspace')

# Query to check H3_10 indices in Cassandra
for h3_10 in h3_10_list:
    query_cassandra = f"SELECT h3_index FROM your_table WHERE h3_index = '{h3_10}'"
    rows = session.execute(query_cassandra)
    
    if rows.one():
        matching_h3_10_cassandra.append(h3_10)
    else:
        non_matching_h3_10_cassandra.append(h3_10)

# Print results
print("BigQuery Results:")
print(f"Matching H3_10 indices: {matching_h3_10_bq}")
print(f"Non-matching H3_10 indices: {non_matching_h3_10_bq}")

print("\nCassandra Results:")
print(f"Matching H3_10 indices: {matching_h3_10_cassandra}")
print(f"Non-matching H3_10 indices: {non_matching_h3_10_cassandra}")
