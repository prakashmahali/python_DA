import subprocess

# Prepare a single SQL query with all H3_10 indices
h3_10_str = ', '.join(f"'{h3}'" for h3 in h3_10_indices)
bq_query = f"""
    SELECT h3_index FROM `your_project.your_dataset.your_table`
    WHERE h3_index IN ({h3_10_str})
"""

# Run the query using subprocess (bq query command)
process = subprocess.Popen(
    ['bq', 'query', '--use_legacy_sql=false', '--format=csv', bq_query],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)
output, error = process.communicate()

# Decode and clean the output, remove header, split lines, and remove any pipe '|' symbols
output_str = output.decode('utf-8').strip().splitlines()
bq_results = [line.replace('|', '').strip() for line in output_str[1:]]  # Remove header and pipe chars

# Compare H3_10 indices with BQ results
matching_bq = [h3 for h3 in h3_10_indices if h3 in bq_results]
non_matching_bq = [h3 for h3 in h3_10_indices if h3 not in bq_results]

print(f"Matching H3_10 indices in BigQuery: {matching_bq}")
print(f"Non-matching H3_10 indices in BigQuery: {non_matching_bq}")
