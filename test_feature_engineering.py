import unittest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from etl_utils import Feature_Engineering

class FeatureEngineeringTests(unittest.TestCase):
    def setUp(self):
        # Create a sample DataFrame for testing
        data = {
            'Symbol': ['ABC', 'ABC', 'DEF', 'DEF'],
            'Volume': [100, 200, 300, 400],
            'Adj Close': [10.0, 12.0, 20.0, 18.0]
        }
        self.df = pd.DataFrame(data)

    def test_vol_moving_avg_calculation(self):
        # Perform the feature engineering
        output_file = Feature_Engineering(self.df)

        # Read the output file into a DataFrame
        df_output = pq.read_table(output_file).to_pandas()

        # Verify the vol_moving_avg column is correctly calculated
        expected_vol_moving_avg = [100.0, 150.0, 300.0, 350.0]
        self.assertListEqual(
            df_output['vol_moving_avg'].tolist(),
            expected_vol_moving_avg,
            msg="Incorrect vol_moving_avg calculation"
        )

    def test_adj_close_rolling_med_calculation(self):
        # Perform the feature engineering
        output_file = Feature_Engineering(self.df)

        # Read the output file into a DataFrame
        df_output = pq.read_table(output_file).to_pandas()

        # Verify the adj_close_rolling_med column is correctly calculated
        expected_adj_close_rolling_med = [10.0, 11.0, 20.0, 19.0]
        self.assertListEqual(
            df_output['adj_close_rolling_med'].tolist(),
            expected_adj_close_rolling_med,
            msg="Incorrect adj_close_rolling_med calculation"
        )

if __name__ == '__main__':
    unittest.main()
