import h3
import subprocess
import cassandra
from cassandra.cluster import Cluster

# Step 1: Input H3_6 index
h3_6 = '862a10723ffffff'

# Convert H3_6 to H3_10 indices
h3_10_indices = h3.h3_to_children(h3_6, 10)
h3_10_set = set(h3_10_indices)

print(f"H3 Index at resolution 6: {h3_6}")
print(f"All H3_10 indices: {h3_10_indices}")

# Step 2: Create BigQuery SQL query
bq_table = 'your_project.your_dataset.your_table'
h3_10_str = ', '.join(f"'{h3_10}'" for h3_10 in h3_10_indices)

bq_query = f"""
    SELECT h3_index FROM `{bq_table}`
    WHERE h3_index IN ({h3_10_str})
"""

# Step 3: Execute the BQ query
bq_command = f"bq query --use_legacy_sql=false \"{bq_query}\""
bq_result = subprocess.run(bq_command, shell=True, capture_output=True, text=True)

# Step 4: Process the BQ output
bq_h3_list = bq_result.stdout.splitlines()
bq_h3_set = {line.strip().replace('|', '') for line in bq_h3_list if line.strip()}

# Step 5: Determine matching and non-matching H3_10 indices for BQ
matching_h3_10_bq = h3_10_set.intersection(bq_h3_set)
non_matching_h3_10_bq = h3_10_set.difference(bq_h3_set)

print("\nBQ Results:")
print("Matching H3_10:", matching_h3_10_bq)
print("Non-Matching H3_10:", non_matching_h3_10_bq)

# Step 6: Compare with Cassandra
# Connect to Cassandra
cluster = Cluster(['cassandra_host'])  # replace with your Cassandra host
session = cluster.connect('your_keyspace')

# Create a Cassandra query
cassandra_query = f"""
    SELECT h3_index FROM your_cassandra_table
    WHERE h3_index IN ({h3_10_str})
"""

# Execute the Cassandra query
cassandra_rows = session.execute(cassandra_query)
cassandra_h3_set = {row.h3_index for row in cassandra_rows}

# Step 7: Determine matching and non-matching H3_10 indices for Cassandra
matching_h3_10_cassandra = h3_10_set.intersection(cassandra_h3_set)
non_matching_h3_10_cassandra = h3_10_set.difference(cassandra_h3_set)

print("\nCassandra Results:")
print("Matching H3_10:", matching_h3_10_cassandra)
print("Non-Matching H3_10:", non_matching_h3_10_cassandra)

# Close the Cassandra connection
cluster.shutdown()
