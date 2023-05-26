import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
import logging

def Raw_Data_Processing(folder_path):
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
    combined_data = pd.concat(data, ignore_index=True)

    # Convert the DataFrame to a PyArrow table
    table = pa.Table.from_pandas(combined_data)

    # Specify the output file path for the Parquet file
    output_file = r'output\etf_data.parquet'

    # Write the PyArrow table to a Parquet file
    pq.write_table(table, output_file)

    return output_file


def Feature_Engineering(input_file):
    # Load the Parquet file into a pandas DataFrame
    df = pq.read_table(input_file).to_pandas()

    # Calculate the moving average of trading volume (Volume) over 30 days for each stock and ETF
    df['vol_moving_avg'] = df.groupby('Symbol')['Volume'].rolling(window=30, min_periods=1).mean().reset_index(level=0, drop=True)

    # Calculate the rolling median of the 'Adj Close' price over 30 days for each stock and ETF
    df['adj_close_rolling_med'] = df.groupby('Symbol')['Adj Close'].rolling(window=30, min_periods=1).median().reset_index(level=0, drop=True)

    # Save the updated DataFrame back to a Parquet file
    output_file = r'output\etf_data_with_features.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file)

    return output_file


def Integrate_ML_Training(input_file):
    # Configure logging
    logging.basicConfig(filename='etl_process.log', level=logging.INFO)

    logging.info("Raw Data Processing...")
    output_file_raw_data = Raw_Data_Processing(input_file)
    logging.info(f"Raw Data Processing completed. Output file: {output_file_raw_data}")

    logging.info("Feature Engineering...")
    output_file_feature_engineering = Feature_Engineering(output_file_raw_data)
    logging.info(f"Feature Engineering completed. Output file: {output_file_feature_engineering}")

    # Load the Parquet file into a pandas DataFrame
    df = pq.read_table(output_file_feature_engineering).to_pandas()

    # Assume `data` is loaded as a Pandas DataFrame
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)

    # Remove rows with NaN values
    df.dropna(inplace=True)

    # Select features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'

    X = df[features]
    y = df[target]

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Create a RandomForestRegressor model
    model = RandomForestRegressor(n_estimators=100, random_state=42)

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on test data
    y_pred = model.predict(X_test)

    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    # Log the metrics to a file
    logging.info(f"Mean Absolute Error: {mae}")
    logging.info(f"Mean Squared Error: {mse}")

    # Save the model to disk
    model_path = r'output\models\random_forest_model.pkl'
    joblib.dump(model, model_path)

    logging.info(f"Integrating ML Training completed. Model saved to: {model_path}")

    return model_path


# Specify the input file path for the Parquet file
input_file = r'etfs'

# Run the ETL process
model_path = Integrate_ML_Training(input_file)

# Print the model path
print(model_path)
