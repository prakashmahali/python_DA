import subprocess
import pandas as pd
import io

# Credentials and URL
username = "your_username"
password = "your_password"
url = "https://your_url"

# Curl command with authentication and data parameters
cmd = 'curl -k -u "{}":"{}" {} -d search="|inputlookup gsam_mtn.csv" -d output_mode=csv -d count=0 -d offset=25000'.format(username, password, url)

# Execute the curl command
result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

# Convert the output to a Pandas DataFrame
data = result.stdout

# Read the CSV output into a Pandas DataFrame
df = pd.read_csv(io.StringIO(data))

# Display DataFrame
print(df.head())
