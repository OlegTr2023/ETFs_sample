import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Specify the path to the folder containing the ETF files
folder_path = r'etfs'

# Create an empty list to store the data from all files
data = []

# Iterate over each file in the folder
for filename in os.listdir(folder_path):
    file_path = os.path.join(folder_path, filename)
    
    # Extract the sheet name from the file name
    sheet_name = os.path.splitext(filename)[0]
    
    # Read the file as a pandas DataFrame with the sheet name as 'Symbol'
    df = pd.read_csv(file_path)
    df['Symbol'] = sheet_name

    security_name = f"Security-{sheet_name}"
    df['Security Name'] = security_name
    
    # Extract the required columns
    df = df[['Symbol', 'Security Name', 'Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
    
    # Append the DataFrame to the data list
    data.append(df)

# Concatenate all DataFrames into a single DataFrame
combined_data = pd.concat(data)

# Convert the DataFrame to a PyArrow table
table = pa.Table.from_pandas(combined_data)

# Specify the output file path for the Parquet file
output_file = r'output\etf_data.parquet'

# Write the PyArrow table to a Parquet file
pq.write_table(table, output_file)

# Optional: Read the Parquet file back into a PyArrow table
read_table = pq.read_table(output_file)

# Optional: Convert the PyArrow table back to a pandas DataFrame
df = read_table.to_pandas()

# Print the resulting DataFrame
print(df)
