# ETL Process for ETF Data Analysis

This repository contains the code for an ETL (Extract, Transform, Load) process to analyze ETF (Exchange-Traded Fund) data. The process involves raw data processing, feature engineering, and integration of machine learning training.

## Description

The ETL process consists of three main steps:

1. Raw Data Processing: Takes ETF data from multiple files, combines them into a single dataset, and saves it in the Parquet format.

2. Feature Engineering: Applies data transformations on the raw dataset to calculate additional features such as moving average and rolling median, and saves the updated dataset.

3. Integration of ML Training: Loads the feature-engineered dataset, performs machine learning training using a Random Forest regressor model, and saves the trained model to disk.

## Prerequisites

- Python (version 3.7)
- Pandas (version 1.3.5)
- PyArrow (version 12.0.0)
- scikit-learn (version 1.0.2)
- joblib (version 1.2.0)

## Usage

1. Clone the repository:


  git clone https://github.com/OlegTr2023/ETFs_sample


2. Install the required dependencies:

  pip install -r requirements.txt


3. Update the folder path in the `main.py` file:

```python
input_file = r'etfs'

Run the ETL process:

python main.py 
```

4. Check the logs:
    The ETL process logs will be saved in the etl_process.log file.

5. Check the output files:
    The raw data processed file will be saved as output/etf_data.parquet.
    The feature-engineered file will be saved as output/etf_data_with_features.parquet.
    The trained machine learning model will be saved as random_forest_model.pkl.
    The training metrics will be saved to etl_process.log file.

# Feature Engineering Tests

This project contains unit tests for the feature engineering logic in the Feature_Engineering function. The tests validate the calculation of the moving average and rolling median for the given DataFrame.

## Prerequisites

Make sure you have the following dependencies installed:

- Python (version 3.0 or above)
- pandas
- pyarrow

## Running the Tests
To run the unit tests, follow these steps:

1. Open a terminal or command prompt and navigate to the project directory.
2. Execute the following command to run the tests:

```python
python -m unittest feature_engineering_tests.py 
```

This command will execute the test cases defined in the feature_engineering_tests.py file.

View the test results in the terminal/command prompt. Any failures or errors will be displayed with relevant error messages.

## Test Cases

The following test cases are included:

1. test_vol_moving_avg_calculation: Verifies the correctness of the vol_moving_avg column calculation.
2. test_adj_close_rolling_med_calculation: Verifies the correctness of the adj_close_rolling_med column calculation.
For each test case, expected values are compared with the actual results obtained from the feature engineering process. Any discrepancies will trigger test failures.


# Model Serving

This project implements an API service to serve a trained predictive model. The API provides an /predict endpoint that takes two values, vol_moving_avg and adj_close_rolling_med, and responds with an integer value representing the predicted trading volume.

## API Endpoint

/predict

This endpoint accepts a GET request with the following query parameters:

vol_moving_avg (float): The value of vol_moving_avg.
adj_close_rolling_med (float): The value of adj_close_rolling_med.
The API will use the provided values to make a prediction using the trained model and return an HTTP response with the predicted trading volume as the response body.

Example

Request:
GET /predict?vol_moving_avg=12345&adj_close_rolling_med=25

https://random-forest-model.onrender.com/predict?vol_moving_avg=12345&adj_close_rolling_med=25

Response:
12320

The response body contains the predicted trading volume as an integer value.

## Deployment

The API service is deployed on Render.com and can be accessed using the following URL:

To make predictions, send a GET request to the /predict endpoint of the deployed API service with the required query parameters.

## Setup and Custom Deployment

If you want to deploy the API service on your own infrastructure, follow these steps:

1. Install the required dependencies specified in the project's requirements.txt file.

2. Set up the trained predictive model. Ensure that the model file is available in the appropriate location.

3. Run the API service using the provided command or script - app.py.

4. The API service will start and listen for incoming requests on a specific port (e.g., port 5000). You can customize the port as per your requirements.

5. Send a GET request to the /predict endpoint with the required query parameters to make predictions.

## Conclusion

This API service provides a simple and efficient way to interact with the trained predictive model and obtain trading volume predictions based on the given values of vol_moving_avg and adj_close_rolling_med. You can use the deployed service on Render.com or set up your own custom deployment. Integrate the API service into your applications or systems to make accurate trading volume predictions.