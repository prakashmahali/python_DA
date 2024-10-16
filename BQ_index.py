import h3

# Input H3 index at resolution 6
h3_6 = '862a10723ffffff'

# Convert H3_6 to H3_10 (get all child H3_10 indices)
h3_10_indices = h3.h3_to_children(h3_6, 10)

# Count the number of H3_10 indices
h3_10_count = len(h3_10_indices)

print(f"H3 Index at resolution 6: {h3_6}")
print(f"Number of H3_10 indices under H3_6: {h3_10_count}")
print(f"All H3_10 indices: {h3_10_indices}")


import os

# Example: BigQuery table and dataset details
bq_table = 'your_project.your_dataset.your_table'

# Iterate over each H3_10 index and check if it exists in the BigQuery table
for h3_10 in h3_10_indices:
    query = f"""
        SELECT COUNT(*) FROM `{bq_table}`
        WHERE h3_index = '{h3_10}'
    """

    # Run the query using the bq command-line tool
    result = os.popen(f"bq query --use_legacy_sql=false \"{query}\"").read()

    # Check if the H3_10 index was found
    if '0' in result:
        print(f"H3_10 index {h3_10} not found in BigQuery")
    else:
        print(f"H3_10 index {h3_10} found in BigQuery")
