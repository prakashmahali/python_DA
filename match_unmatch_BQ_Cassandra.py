import h3
import os
from cassandra.cluster import Cluster

# Convert H3_6 to H3_10
def get_h3_10_indices(h3_6):
    return h3.h3_to_children(h3_6, 10)

# Check if H3_10 exists in BigQuery table using bq query command
def check_h3_in_bq(h3_10, bq_table):
    query = f"SELECT COUNT(*) FROM `{bq_table}` WHERE h3_index = '{h3_10}'"
    result = os.popen(f"bq query --use_legacy_sql=false \"{query}\"").read()
    return '0' not in result  # Returns True if found, False otherwise

# Check if H3_10 exists in Cassandra table
def check_h3_in_cassandra(h3_10, cassandra_session, keyspace, table):
    query = f"SELECT COUNT(*) FROM {keyspace}.{table} WHERE h3_index = '{h3_10}'"
    rows = cassandra_session.execute(query)
    return rows[0].count > 0  # Returns True if found, False otherwise

# Main function to compare H3_10 indices with BQ and Cassandra
def compare_h3_indices(h3_6, bq_table, cassandra_session, keyspace, table):
    h3_10_indices = get_h3_10_indices(h3_6)
    matching_bq = []
    non_matching_bq = []
    matching_cassandra = []
    non_matching_cassandra = []
    
    for h3_10 in h3_10_indices:
        # Check in BigQuery
        if check_h3_in_bq(h3_10, bq_table):
            matching_bq.append(h3_10)
        else:
            non_matching_bq.append(h3_10)

        # Check in Cassandra
        if check_h3_in_cassandra(h3_10, cassandra_session, keyspace, table):
            matching_cassandra.append(h3_10)
        else:
            non_matching_cassandra.append(h3_10)

    # Print results
    print(f"Matching H3_10 in BigQuery: {matching_bq}")
    print(f"Non-Matching H3_10 in BigQuery: {non_matching_bq}")
    print(f"Matching H3_10 in Cassandra: {matching_cassandra}")
    print(f"Non-Matching H3_10 in Cassandra: {non_matching_cassandra}")

# Connect to Cassandra
def connect_to_cassandra():
    cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra cluster IP
    session = cluster.connect('your_keyspace')  # Replace with your keyspace
    return session

if __name__ == "__main__":
    # Example inputs
    h3_6 = '862a10723ffffff'
    bq_table = 'your_project.your_dataset.your_table'
    
    # Cassandra table info
    cassandra_keyspace = 'your_keyspace'
    cassandra_table = 'your_table'

    # Cassandra connection
    cassandra_session = connect_to_cassandra()

    # Compare H3_10 indices with BigQuery and Cassandra
    compare_h3_indices(h3_6, bq_table, cassandra_session, cassandra_keyspace, cassandra_table)
