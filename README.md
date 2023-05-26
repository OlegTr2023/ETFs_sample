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