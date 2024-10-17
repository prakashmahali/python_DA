import h3
import subprocess

def convert_h3_to_lower_resolution(h3_index, from_resolution, to_resolution):
    return h3.h3_to_parent(h3_index, to_resolution)

def generate_h3_children(h3_index, from_resolution, to_resolution):
    children = h3.h3_to_children(h3_index, to_resolution)
    return children

def compare_with_bq_table(project_id, dataset_id, table_name, generated_indices):
    # Construct the BigQuery query
    query = f"""
        SELECT h3_index 
        FROM `{project_id}.{dataset_id}.{table_name}`
        WHERE h3_index IN ('{ "','".join(generated_indices)}')
    """
    
    # Execute the BigQuery query using bq command-line tool
    command = f"bq query --project_id={project_id} '{query}'"
    
    try:
        output = subprocess.check_output(command, shell=True).decode('utf-8')
        matching_indices = [line.split()[0] for line in output.split('\n') if line.strip()]
        return matching_indices
    except subprocess.CalledProcessError as e:
        print(f"Error executing query: {e}")
        return []

# Example usage
h3_index_10 = "8a2a1072bffffff"
h3_index_6 = convert_h3_to_lower_resolution(h3_index_10, 10, 6)
print(f"H3 Index at resolution 6: {h3_index_6}")

children
