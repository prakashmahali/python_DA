from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Step 1: Configure your connection parameters
CASSANDRA_HOST = 'your_cassandra_host'  # Replace with your Cassandra host
USERNAME = 'your_username'               # Replace with your username
PASSWORD = 'your_password'               # Replace with your password

# Step 2: Set up the authentication provider
auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)

# Step 3: Create a cluster instance
cluster = Cluster([CASSANDRA_HOST], auth_provider=auth_provider)

# Step 4: Connect to the session
try:
    session = cluster.connect('your_keyspace')  # Replace with your keyspace
    print("Connected to Cassandra")

    # Example query
    rows = session.execute("SELECT * FROM your_table")  # Replace with your table
    for row in rows:
        print(row)

except Exception as e:
    print(f"Error connecting to Cassandra: {e}")
finally:
    # Close the session and cluster connection
    if 'session' in locals():
        session.shutdown()
    if 'cluster' in locals():
        cluster.shutdown()
