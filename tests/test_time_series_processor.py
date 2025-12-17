import unittest
import pandas as pd
from io import StringIO

from src.asset_base.time_series_processor import TimeSeriesProcessor


class TestTimeSeriesProcessor(unittest.TestCase):
    """Test TimeSeriesProcessor functionality."""

    @classmethod
    def setUpClass(cls):
        """Set up class-wide test fixtures."""
        cls.identity_code = "XNYS:ABC"  # ABC Inc. identity code as example

        # Price data fixture - days of trade data with holidays and anomalies
        price_csv = (
            "identity_code,date_stamp,price,anomaly,anomaly_value,holiday\n"
            f"{cls.identity_code},2020-12-01,123.0,,,\n"
            f"{cls.identity_code},2020-12-02,124.0,,,\n"
            f"{cls.identity_code},2020-12-03,125.0,,,\n"
            f"{cls.identity_code},2020-12-04,126.0,spike,5.0,\n"  # Friday spike
            f"{cls.identity_code},2020-12-05,,,,Saturday\n"
            f"{cls.identity_code},2020-12-06,,,,Sunday\n"
            f"{cls.identity_code},2020-12-07,129.0,,,\n"
            f"{cls.identity_code},2020-12-08,130.0,,,\n"
            f"{cls.identity_code},2020-12-09,131.0,spike,5.0,\n"  # Midweek spike
            f"{cls.identity_code},2020-12-10,132.0,,,\n"
            f"{cls.identity_code},2020-12-11,133.0,,,\n"
            f"{cls.identity_code},2020-12-12,,,,Saturday\n"
            f"{cls.identity_code},2020-12-13,,,,Sunday\n"
            f"{cls.identity_code},2020-12-14,136.0,spike,5.0,\n"  # Monday spike
            f"{cls.identity_code},2020-12-15,137.0,,,\n"
            f"{cls.identity_code},2020-12-16,,,,Day of Reconciliation\n"
            f"{cls.identity_code},2020-12-17,139.0,,,\n"
            f"{cls.identity_code},2020-12-18,140.0,spike,5.0,\n"  # Double spike Friday
            f"{cls.identity_code},2020-12-19,,,,Saturday\n"
            f"{cls.identity_code},2020-12-20,,,,Sunday\n"
            f"{cls.identity_code},2020-12-21,143.0,spike,5.0,\n"  # ...and Monday
            f"{cls.identity_code},2020-12-22,144.0,,,\n"
            f"{cls.identity_code},2020-12-23,145.0,,,\n"
            f"{cls.identity_code},2020-12-24,146.0,jump,10.0,\n"  # Xmas Eve jump
            f"{cls.identity_code},2020-12-25,,,,Christmas Day\n"
            f"{cls.identity_code},2020-12-26,,,,Saturday\n"
            f"{cls.identity_code},2020-12-27,,,,Sunday\n"
            f"{cls.identity_code},2020-12-28,150.0,,,\n"
            f"{cls.identity_code},2020-12-29,151.0,,,\n"
            f"{cls.identity_code},2020-12-30,152.0,,,\n"
            f"{cls.identity_code},2020-12-31,153.0,,,\n"
            f"{cls.identity_code},2021-01-01,,,,New Year's Day\n"
            f"{cls.identity_code},2021-01-02,,,,Saturday\n"
            f"{cls.identity_code},2021-01-03,,,,Sunday\n"
            f"{cls.identity_code},2021-01-04,157.0,,,\n"
            f"{cls.identity_code},2021-01-05,158.0,,,\n"
            f"{cls.identity_code},2021-01-06,159.0,,,\n"
            f"{cls.identity_code},2021-01-07,160.0,,,\n"
            f"{cls.identity_code},2021-01-08,161.0,,,\n"
            f"{cls.identity_code},2021-01-09,,,,Saturday\n"
            f"{cls.identity_code},2021-01-10,,,,Sunday\n"
        )
        price_df = pd.read_csv(StringIO(price_csv))
        price_df['date_stamp'] = pd.to_datetime(price_df['date_stamp'])
        cls.plain_test_price_df = price_df[['identity_code', 'date_stamp', 'price']]

        # Need an index for applying anomalies
        price_df = price_df.set_index(['identity_code', 'date_stamp'])
        # Apply price spikes on the day they occur
        spike_df = price_df.loc[price_df['anomaly'] == 'spike', 'anomaly_value']
        price_df.loc[price_df['anomaly'] == 'spike', 'price'] += spike_df
        # Apply price jumps by accumulating jumps over time
        jump_df = price_df.loc[price_df['anomaly'] == 'jump', 'anomaly_value']
        jump_df = jump_df.reindex(price_df.index, fill_value=0.0)
        jump_df = jump_df.cumsum()
        price_df['price'] += jump_df
        # Reset index
        price_df = price_df.reset_index()
        cls.outlier_test_price_df = price_df[['identity_code', 'date_stamp', 'price']]

        # Assert that days with holiday names are NaN in price
        assert price_df.loc[price_df['holiday'].notna(), 'price'].isna().all(), "Holiday prices should be NaN."

        # Price holidays bool index series
        cls.test_holidays_sr = price_df['holiday'].isna()

        # Dividend data fixture - 2 dividend events
        dividend_csv = (
            "identity_code,date_stamp,adjusted_value\n"
            f"{cls.identity_code},2020-12-03,1.25\n"
            f"{cls.identity_code},2020-12-04,1.25\n"
        )
        dividend_df = pd.read_csv(StringIO(dividend_csv))
        dividend_df['date_stamp'] = pd.to_datetime(dividend_df['date_stamp'])
        cls.test_dividend_df = dividend_df

        # Split data fixture - 2 split events
        split_csv = (
            "identity_code,date_stamp,numerator,denominator\n"
            f"{cls.identity_code},2020-12-07,2.0,1.0\n"
            f"{cls.identity_code},2020-12-09,4.0,1.0\n"
        )
        split_df = pd.read_csv(StringIO(split_csv))
        split_df['date_stamp'] = pd.to_datetime(split_df['date_stamp'])
        cls.test_split_df = split_df

    def setUp(self):
        """Set up test fixtures for each test method."""
        # Create fresh copies of the test data for each test
        pass

    def tearDown(self):
        """Clean up after each test method."""
        # Clear any instance variables
        pass

    def test_show_price_df_fixture(self):
        """Print the price fixture DataFrame for visual inspection."""
        print("\nPlain Test Price DataFrame:")
        print(self.plain_test_price_df)
        print("\nOutlier Test Price DataFrame:")
        print(self.outlier_test_price_df)

    def test_constructor(self):
        """Test TimeSeriesProcessor constructor."""
        tsp = TimeSeriesProcessor(self.plain_test_price_df)
        self.assertIsInstance(tsp, TimeSeriesProcessor)
        self.assertTrue(hasattr(tsp, 'prices_df'))
        self.assertTrue(hasattr(tsp, 'identity_codes'))
        self.assertListEqual(
            tsp.identity_codes,
            [self.identity_code]
        )




if __name__ == "__main__":
    unittest.main()
