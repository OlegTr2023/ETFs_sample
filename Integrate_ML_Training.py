import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
#from sklearn.externals import joblib
import joblib
import logging

# Configure logging
logging.basicConfig(filename='training.log', level=logging.INFO)

input_data = r'output\etf_data_with_vol_moving_avg_3_files.parquet'

# Optional: Read the Parquet file back into a PyArrow table
read_table = pq.read_table(input_data)

# Optional: Convert the PyArrow table back to a pandas DataFrame
data = read_table.to_pandas()

# Assume `data` is loaded as a Pandas DataFrame
data['Date'] = pd.to_datetime(data['Date'])
data.set_index('Date', inplace=True)

# Remove rows with NaN values
data.dropna(inplace=True)

# Select features and target
features = ['vol_moving_avg', 'adj_close_rolling_med']
target = 'Volume'

X = data[features]
y = data[target]

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
model_path = 'random_forest_model.pkl'
joblib.dump(model, model_path)

