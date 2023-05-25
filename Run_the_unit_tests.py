import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

input_file = r'C:\ProjectVS\parquet\etf_data_with_vol_moving_avg_3_files.parquet'

# Load the Parquet file into a pandas DataFrame
df = pq.read_table(input_file).to_pandas()

# Function to calculate the moving average of trading volume
def calculate_vol_moving_avg(df):
    df['vol_moving_avg'] = df.groupby('Symbol')['Volume'].rolling(window=30, min_periods=1).mean().reset_index(level=0, drop=True)
    return df

# Function to calculate the rolling median of Adj Close price
def calculate_adj_close_rolling_med(df):
    df['adj_close_rolling_med'] = df.groupby('Symbol')['Adj Close'].rolling(window=30, min_periods=1).median().reset_index(level=0, drop=True)
    return df

# Example unit test for calculate_vol_moving_avg function
def test_calculate_vol_moving_avg():
    # Create a test DataFrame with sample data
    test_df = pd.DataFrame({
        'Symbol': ['AAA', 'AAA', 'BBB', 'BBB'],
        'Volume': [100, 200, 300, 400]
    })

    # Call the function to calculate vol_moving_avg
    result_df = calculate_vol_moving_avg(test_df)

    # Verify the expected result
    expected_df = pd.DataFrame({
        'Symbol': ['AAA', 'AAA', 'BBB', 'BBB'],
        'Volume': [100, 200, 300, 400],
        'vol_moving_avg': [100.0, 150.0, 300.0, 350.0]
    })
    assert result_df.equals(expected_df)


# Example unit test for calculate_adj_close_rolling_med function
def test_calculate_adj_close_rolling_med():
    # Create a test DataFrame with sample data
    test_df = pd.DataFrame({
        'Symbol': ['AAA', 'AAA', 'BBB', 'BBB'],
        'Adj Close': [10.0, 20.0, 30.0, 40.0]
    })

    # Call the function to calculate adj_close_rolling_med
    result_df = calculate_adj_close_rolling_med(test_df)

    # Verify the expected result
    expected_df = pd.DataFrame({
        'Symbol': ['AAA', 'AAA', 'BBB', 'BBB'],
        'Adj Close': [10.0, 20.0, 30.0, 40.0],
        'adj_close_rolling_med': [10.0, 15.0, 30.0, 35.0]
    })
    assert result_df.equals(expected_df)

# Run the unit tests
if __name__ == '__main__':
    pytest.main()