import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def Feature_Engineering(df):
    # Calculate the moving average of trading volume (Volume) over 30 days for each stock and ETF
    df['vol_moving_avg'] = df.groupby('Symbol')['Volume'].rolling(window=30, min_periods=1).mean().reset_index(level=0, drop=True)

    # Calculate the rolling median of the 'Adj Close' price over 30 days for each stock and ETF
    df['adj_close_rolling_med'] = df.groupby('Symbol')['Adj Close'].rolling(window=30, min_periods=1).median().reset_index(level=0, drop=True)
    
    # Save the updated DataFrame back to a Parquet file
    output_file = r'output\etf_data_with_vol_moving_avg_test.parquet'
    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file)

    # Return output_file
    return output_file

