import subprocess

def run_bq_query(query):
    try:
        # Create the bq command
        command = ['bq', 'query', '--nouse_legacy_sql', query]
        
        # Execute the command
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Check for errors
        if result.returncode != 0:
            print(f"Error executing query: {result.stderr}")
        else:
            print(f"Query result:\n{result.stdout}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example query
sql_query = "SELECT * FROM `your-project.your-dataset.your-table` LIMIT 10"
run_bq_query(sql_query)
