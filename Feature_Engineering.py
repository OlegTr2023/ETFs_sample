import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Specify the input file path for the Parquet file
#input_file = r'C:\ProjectVS\parquet\etf_data.parquet'
input_file = r'C:\ProjectVS\parquet\etf_data_3_files.parquet'


# Load the Parquet file into a pandas DataFrame
df = pq.read_table(input_file).to_pandas()

# Calculate the moving average of trading volume (Volume) over 30 days for each stock and ETF
df['vol_moving_avg'] = df.groupby('Symbol')['Volume'].rolling(window=30, min_periods=1).mean().reset_index(level=0, drop=True)

# Calculate the rolling median of the 'Adj Close' price over 30 days for each stock and ETF
df['adj_close_rolling_med'] = df.groupby('Symbol')['Adj Close'].rolling(window=30, min_periods=1).median().reset_index(level=0, drop=True)

# Save the updated DataFrame back to a Parquet file
output_file = r'C:\ProjectVS\parquet\etf_data_with_vol_moving_avg_3_files.parquet'
table = pa.Table.from_pandas(df)
#table = pq.Table.from_pandas(df)
pq.write_table(table, output_file)

# Optional: Read the Parquet file back into a PyArrow table
read_table = pq.read_table(output_file)

# Optional: Convert the PyArrow table back to a pandas DataFrame
df = read_table.to_pandas()

# Print the resulting DataFrame
print(df)
