import h3
import subprocess

# Input H3_6 index
h3_6 = '862a10723ffffff'

# Convert H3_6 to H3_10 (get all child H3_10 indices)
h3_10_indices = h3.h3_to_children(h3_6, 10)

# Lists to store matching and non-matching H3_10 indices
matching_h3_10 = []
non_matching_h3_10 = []

# BigQuery table details
bq_table = 'your_project.your_dataset.your_table'

# Check each H3_10 index in BigQuery
for h3_10 in h3_10_indices:
    query = f"""
        SELECT COUNT(*)
        FROM `{bq_table}`
        WHERE h3_index = '{h3_10}'
    """
    
    # Run the query using the bq command-line tool
    result = subprocess.getoutput(f'bq query --use_legacy_sql=false "{query}"')
    
    # Check if H3_10 index was found (assuming result returns a number like 0 or 1)
    if '0' in result:
        non_matching_h3_10.append(h3_10)
    else:
        matching_h3_10.append(h3_10)

# Print the results
print(f"Matching H3_10 indices: {matching_h3_10}")
print(f"Non-matching H3_10 indices: {non_matching_h3_10}")
