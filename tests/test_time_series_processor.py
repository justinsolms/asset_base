import unittest
import numpy as np
import pandas as pd
from io import StringIO

from src.asset_base.time_series_processor import TimeSeriesProcessor


class TestTimeSeriesProcessor(unittest.TestCase):
    """Test TimeSeriesProcessor functionality."""

    @classmethod
    def setUpClass(cls):
        """Set up class-wide test fixtures."""
        cls.identity_code = "XNYS:ABC"  # ABC Inc. identity code as example

        # Price data fixture - days of trade data with holidays and anomalies In
        # prices spikes cause twin outliers on the spike day and on the
        # following day unless there are twin spikes. Twin spikes may not look
        # like twins until the holiday or missing data rows between the twins
        # are removed. Jumps cause a single outlier on the jump day.
        fixture_df = pd.read_csv("tests/fixtures/time_series_processor_price_fixture.csv")
        price_df = fixture_df[['identity_code', 'date_stamp', 'price', 'anomaly', 'anomaly_value', 'holiday', 'is_outlier']]
        price_df['date_stamp'] = pd.to_datetime(price_df['date_stamp'])
        cls.test_price_df = price_df.copy()

        # Create clean price DataFrame without anomalies
        cls.clean_test_price_df = price_df[['identity_code', 'date_stamp', 'price']]

        # Create dirty price DataFrame with anomalies applied
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
        # Dirty price DataFrame with anomalies applied
        cls.dirty_test_price_df = price_df[['identity_code', 'date_stamp', 'price']]

        # Create a tsp with insufficient rows to meet sample size adequacy
        # for multiple assets test later
        cls.insufficient_test_price_df = price_df[['identity_code', 'date_stamp', 'price']].iloc[:5]

        # Assert that days with holiday names are NaN in price as there would be
        # no trading on those days
        assert price_df.loc[price_df['holiday'].notna(), 'price'].isna().all(), "Holiday prices should be NaN."

        # Price holidays bool index series
        cls.test_holidays_sr = price_df['holiday'].isna()

        # Dividend data fixture - 2 dividend events
        dividend_df = fixture_df[['identity_code', 'date_stamp', 'dividend']]
        dividend_df.rename(columns={'dividend': 'unadjusted_value'}, inplace=True)
        dividend_df['date_stamp'] = pd.to_datetime(dividend_df['date_stamp'])
        cls.test_dividend_df = dividend_df

        # Split data fixture - 2 split events
        split_df = fixture_df[['identity_code', 'date_stamp', 'numerator', 'denominator']]
        split_df['date_stamp'] = pd.to_datetime(split_df['date_stamp'])
        cls.test_split_df = split_df

        cls.downsample_period_str = "W"
        cls.clean_outliers = True

    def setUp(self):
        """Set up test fixtures for each test method."""
        # Create fresh copies of the test data for each test
        self.tsp_clean = TimeSeriesProcessor(
            self.clean_test_price_df.copy(),
            self.test_dividend_df.copy(),
            self.test_split_df.copy(),
            self.downsample_period_str, self.clean_outliers)
        self.tsp_dirty = TimeSeriesProcessor(
            self.dirty_test_price_df.copy(),
            self.test_dividend_df.copy(),
            self.test_split_df.copy(),
            self.downsample_period_str, self.clean_outliers)

        self.tsp_insufficient = TimeSeriesProcessor(
            self.insufficient_test_price_df.copy(),
            self.test_dividend_df.copy(),
            self.test_split_df.copy(),
            self.downsample_period_str, self.clean_outliers)

    def tearDown(self):
        """Clean up after each test method."""
        # Clear any instance variables
        pass

    def test_show_price_df_fixture(self):
        """Print the price fixture DataFrame for visual inspection."""
        print("\nPlain Test Price DataFrame:")
        print(self.clean_test_price_df)
        print("\nOutlier Test Price DataFrame:")
        print(self.dirty_test_price_df)

    def test_init(self):
        """Test TimeSeriesProcessor initialization."""
        self.assertIsInstance(self.tsp_dirty, TimeSeriesProcessor)
        pd.testing.assert_frame_equal(
            self.tsp_dirty.prices_df.reset_index(drop=True),
            self.dirty_test_price_df.reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            self.tsp_dirty.dividends_df.reset_index(drop=True),
            self.test_dividend_df.reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            self.tsp_dirty.splits_df.reset_index(drop=True),
            self.test_split_df.reset_index(drop=True)
        )
        self.assertEqual(self.tsp_dirty.downsample_period_str, self.downsample_period_str)
        self.assertEqual(self.tsp_dirty.clean_outliers, self.clean_outliers)

    def test_validate_prices(self):
        """Test price validation method."""
        # Validate price test fixtures
        self.tsp_dirty._validate_prices()

        # Validate small price fixture
        invalid_price_df = pd.DataFrame({
            'identity_code': [self.identity_code]*5,
            'date_stamp': pd.date_range(start='2021-01-01', periods=5),
            'price': [100.0, 50.0, 30.0, 200.0, np.nan]
        })
        self.tsp_dirty.prices_df = invalid_price_df
        self.tsp_dirty._validate_prices()

        # Test non-numeric prices
        invalid_price_df = pd.DataFrame({
            'identity_code': [self.identity_code]*5,
            'date_stamp': pd.date_range(start='2021-01-01', periods=5),
            'price': [100.0, 50.0, 'abc', 200.0, np.nan]
        })
        self.tsp_dirty.prices_df = invalid_price_df
        with self.assertRaises(TypeError):
            self.tsp_dirty._validate_prices()

        # Test negative prices
        invalid_price_df = pd.DataFrame({
            'identity_code': [self.identity_code]*5,
            'date_stamp': pd.date_range(start='2021-01-01', periods=5),
            'price': [100.0, -50.0, 30.0, 200.0, np.nan]
        })
        self.tsp_dirty.prices_df = invalid_price_df
        with self.assertRaises(ValueError):
            self.tsp_dirty._validate_prices()

    def test_validate_daily_sampling_frequency(self):
        """Test sampling frequency validation method with daily ("D") data."""
        # Create business daily prices
        self.tsp_dirty._validate_sampling_frequency()
        # Tests that the frequency is set to daily
        self.assertEqual(pd.infer_freq(self.tsp_dirty.prices_df['date_stamp']), 'D')

    def test_validate_business_daily_sampling_frequency(self):
        """Test sampling frequency validation method with business daily ("B"|"C") data."""
        # Create business daily prices by resampling.
        business_daily_df = self.clean_test_price_df.set_index('date_stamp').resample('B').last().reset_index()
        self.tsp_dirty.prices_df = business_daily_df
        # Validate business daily data
        self.tsp_dirty._validate_sampling_frequency()
        # Tests that the frequency is set to daily
        self.assertEqual(pd.infer_freq(self.tsp_dirty.prices_df['date_stamp']), 'B')

    def test_validate_too_high_sampling_frequency(self):
        """Test sampling frequency validation method with high-frequency data."""
        # Create hourly prices by upsampling fixture data with ffill
        hourly_price_df = self.clean_test_price_df.set_index('date_stamp').resample('H').ffill().reset_index()
        self.tsp_dirty.prices_df = hourly_price_df
        # Validate failure on high-frequency data
        with self.assertRaises(ValueError):
            self.tsp_dirty._validate_sampling_frequency()

    def test_validate_almost_daily_sampling_frequency(self):
        """Test sampling frequency validation method with almost daily data."""
        # Create almost daily prices by dropping fixture weekend and holiday by NaNs
        self.tsp_dirty._dropna_prices()
        self.tsp_dirty._validate_sampling_frequency()
        # Tests that the frequency is set to daily
        self.assertEqual(pd.infer_freq(self.tsp_dirty.prices_df['date_stamp']), None)

    def test_dropna_prices(self):
        """Test dropping NaN prices method."""
        # Count NaNs before dropping
        nan_count_before = self.tsp_dirty.prices_df['price'].isna().sum()
        self.tsp_dirty._dropna_prices()
        # Count NaNs after dropping
        nan_count_after = self.tsp_dirty.prices_df['price'].isna().sum()
        # Test that NaN count decreased
        self.assertLess(nan_count_after, nan_count_before)
        # Test that there are no NaNs after dropping
        self.assertFalse(self.tsp_dirty.prices_df['price'].isna().any())

    def test_normalize_and_order_dates(self):
        """Test date normalization and ordering method."""
        # Shuffle the price DataFrame by random sampling to test ordering
        shuffled_price_df = self.clean_test_price_df.sample(frac=1).reset_index(drop=True)
        self.tsp_dirty.prices_df = shuffled_price_df
        self.tsp_dirty._normalize_and_order_dates()
        pd.testing.assert_frame_equal(
            self.tsp_dirty.prices_df.reset_index(drop=True),
            self.clean_test_price_df.sort_values(by=['identity_code', 'date_stamp']).reset_index(drop=True)
        )

    # TODO: These test must use adjusted prices.
    @unittest.skip("These test must use adjusted prices.")
    def test_median_absolute_deviation(self):
        """Test median absolute deviation calculation of clean prices."""
        self.median_clean = 0.1100
        self.mad_clean = 0.3558
        # Drop NaNs first then identify outliers
        self.tsp_dirty._dropna_prices()
        # Test that there are no NaNs after dropping
        self.assertFalse(self.tsp_dirty.prices_df['price'].isna().any())
        # Calculate price differences
        price_diff = self.tsp_dirty.prices_df['price'].diff()
        # Test first diff should be a NaN
        self.assertTrue(np.isnan(price_diff.iloc[0]))
        # Test two more differences for correctness
        self.assertAlmostEqual(price_diff[1], 0.11, places=4)
        self.assertAlmostEqual(price_diff[2], 0.19, places=4)
        median, mad = TimeSeriesProcessor._median_absolute_price_deviation(price_diff)
        # Test median and MAD values
        self.assertAlmostEqual(median, self.median_clean, places=4)
        self.assertAlmostEqual(mad, self.mad_clean, places=4)

    # TODO: These test must use adjusted prices.
    @unittest.skip("These test must use adjusted prices.")
    def test_modified_z_score(self):
        """Test modified z-score calculation."""
        # Drop NaNs first then identify outliers
        self.tsp_dirty._dropna_prices()
        # Test that there are no NaNs after dropping
        self.assertFalse(self.tsp_dirty.prices_df['price'].isna().any())
        # Calculate modified z-scores
        prices = self.tsp_dirty.prices_df['price']
        mod_z_scores = TimeSeriesProcessor._modified_z_score(prices)
        # First one should be NaN
        self.assertTrue(np.isnan(mod_z_scores[0]))
        # Test a known outlier price spike
        self.assertAlmostEqual(mod_z_scores[3],  13.5179, places=4)
        self.assertAlmostEqual(mod_z_scores[4], -12.5905, places=4)

    # TODO: These test must use adjusted prices.
    @unittest.skip("These test must use adjusted prices.")
    def test_identify_outliers(self):
        """Test outlier identification."""
        # Drop NaNs first then identify outliers
        self.tsp_dirty._dropna_prices()
        outlier_df = self.tsp_dirty._identify_outliers(deviation=3.0)
        outlier_df.drop(columns=['price'], inplace=True)
        # Get known outlier bool series from fixture
        clean_test_df = self.test_price_df.dropna(subset=['price'])
        known_outliers = clean_test_df.set_index(['identity_code', 'date_stamp'])['is_outlier'].reset_index()
        # Test that the outlier bool series matches known outliers
        pd.testing.assert_frame_equal(outlier_df, known_outliers)

    def test_check_sample_size_adequacy(self):
        """Test sample size adequacy check method."""
        # Test with single asset and sufficient observations (should pass)
        self.tsp_clean._check_sample_size_adequacy(min_samples_factor=10)

        # Test with insufficient observations using the fixture (should raise ValueError)
        # The insufficient fixture has only 5 price observations for 1 asset
        with self.assertRaises(ValueError) as context:
            self.tsp_insufficient._check_sample_size_adequacy(min_samples_factor=10)
        self.assertIn("Insufficient data", str(context.exception))

    def test_apply_corporate_actions_no_dividends_no_splits(self):
        """Test corporate actions with no dividends and no splits."""
        # Create a simple price series without dividends or splits
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 5,
            'date_stamp': pd.date_range('2020-01-01', periods=5, freq='D'),
            'price': [100.0, 101.0, 102.0, 103.0, 104.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)
        tsp._apply_corporate_actions()

        # Should have dividend and split_ratio columns filled with defaults
        self.assertIn('dividend', tsp.prices_df.columns)
        self.assertIn('split_ratio', tsp.prices_df.columns)
        self.assertIn('total_return', tsp.prices_df.columns)

        # All dividends should be 0.0
        self.assertTrue((tsp.prices_df['dividend'] == 0.0).all())
        # All split_ratios should be 1.0
        self.assertTrue((tsp.prices_df['split_ratio'] == 1.0).all())

        # Total return should be simple price return (no dividends/splits)
        # G_t = 1.0 * (P_t + 0) / P_{t-1} = P_t / P_{t-1}
        expected_returns = [np.nan, 101.0/100.0, 102.0/101.0, 103.0/102.0, 104.0/103.0]
        np.testing.assert_array_almost_equal(
            tsp.prices_df['total_return'].values,
            expected_returns,
            decimal=6
        )

    def test_apply_corporate_actions_with_dividends_only(self):
        """Test corporate actions with dividends but no splits."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 5,
            'date_stamp': pd.date_range('2020-01-01', periods=5, freq='D'),
            'price': [100.0, 101.0, 102.0, 103.0, 104.0]
        })
        # Dividend of 2.0 on day 3 (2020-01-03)
        dividend_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'unadjusted_value': [2.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=None)
        tsp._apply_corporate_actions()

        # Check dividend column
        expected_divs = [0.0, 0.0, 2.0, 0.0, 0.0]
        np.testing.assert_array_almost_equal(
            tsp.prices_df['dividend'].values,
            expected_divs,
            decimal=6
        )

        # Check total returns
        # Day 1: NaN (no prev price)
        # Day 2: 1.0 * (101 + 0) / 100 = 1.01
        # Day 3: 1.0 * (102 + 2) / 101 = 104/101 = 1.029703
        # Day 4: 1.0 * (103 + 0) / 102 = 1.009804
        # Day 5: 1.0 * (104 + 0) / 103 = 1.009709
        expected_returns = [np.nan, 1.01, 104.0/101.0, 103.0/102.0, 104.0/103.0]
        np.testing.assert_array_almost_equal(
            tsp.prices_df['total_return'].values,
            expected_returns,
            decimal=6
        )

    def test_apply_corporate_actions_with_splits_only(self):
        """Test corporate actions with splits but no dividends."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 5,
            'date_stamp': pd.date_range('2020-01-01', periods=5, freq='D'),
            'price': [100.0, 101.0, 50.5, 51.0, 52.0]  # Split on day 3
        })
        # 2-for-1 split on day 3 (2020-01-03)
        split_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=split_df)
        tsp._apply_corporate_actions()

        # Check split_ratio column
        expected_splits = [1.0, 1.0, 2.0, 1.0, 1.0]
        np.testing.assert_array_almost_equal(
            tsp.prices_df['split_ratio'].values,
            expected_splits,
            decimal=6
        )

        # Check total returns
        # Day 1: NaN (no prev price)
        # Day 2: 1.0 * (101 + 0) / 100 = 1.01
        # Day 3: 2.0 * (50.5 + 0) / 101 = 101/101 = 1.0 (split-adjusted)
        # Day 4: 1.0 * (51 + 0) / 50.5 = 1.009901
        # Day 5: 1.0 * (52 + 0) / 51 = 1.019608
        expected_returns = [np.nan, 1.01, 2.0*50.5/101.0, 51.0/50.5, 52.0/51.0]
        np.testing.assert_array_almost_equal(
            tsp.prices_df['total_return'].values,
            expected_returns,
            decimal=6
        )

    def test_apply_corporate_actions_with_dividends_and_splits(self):
        """Test corporate actions with both dividends and splits."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 6,
            'date_stamp': pd.date_range('2020-01-01', periods=6, freq='D'),
            'price': [100.0, 101.0, 102.0, 51.0, 52.0, 53.0]
        })
        # Dividend of 1.0 on day 3
        dividend_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'unadjusted_value': [1.0]
        })
        # 2-for-1 split on day 4
        split_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-04')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=split_df)
        tsp._apply_corporate_actions()

        # Check dividend and split columns
        expected_divs = [0.0, 0.0, 1.0, 0.0, 0.0, 0.0]
        expected_splits = [1.0, 1.0, 1.0, 2.0, 1.0, 1.0]
        np.testing.assert_array_almost_equal(
            tsp.prices_df['dividend'].values, expected_divs, decimal=6
        )
        np.testing.assert_array_almost_equal(
            tsp.prices_df['split_ratio'].values, expected_splits, decimal=6
        )

        # Check total returns
        # Day 1: NaN
        # Day 2: 1.0 * (101 + 0) / 100 = 1.01
        # Day 3: 1.0 * (102 + 1) / 101 = 103/101 = 1.019802
        # Day 4: 2.0 * (51 + 0) / 102 = 102/102 = 1.0
        # Day 5: 1.0 * (52 + 0) / 51 = 1.019608
        # Day 6: 1.0 * (53 + 0) / 52 = 1.019231
        expected_returns = [
            np.nan,
            101.0/100.0,
            103.0/101.0,
            2.0*51.0/102.0,
            52.0/51.0,
            53.0/52.0
        ]
        np.testing.assert_array_almost_equal(
            tsp.prices_df['total_return'].values,
            expected_returns,
            decimal=6
        )

    def test_apply_corporate_actions_multiple_assets(self):
        """Test corporate actions with multiple assets."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A']*3 + ['TEST:B']*3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D').tolist() * 2,
            'price': [100.0, 101.0, 102.0, 200.0, 202.0, 204.0]
        })
        # Dividend for TEST:A only
        dividend_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'unadjusted_value': [1.0]
        })
        # Split for TEST:B only
        split_df = pd.DataFrame({
            'identity_code': ['TEST:B'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=split_df)
        tsp._apply_corporate_actions()

        # TEST:A should have dividend on day 2
        test_a = tsp.prices_df[tsp.prices_df['identity_code'] == 'TEST:A']
        np.testing.assert_array_almost_equal(
            test_a['dividend'].values, [0.0, 1.0, 0.0], decimal=6
        )

        # TEST:B should have split on day 2
        test_b = tsp.prices_df[tsp.prices_df['identity_code'] == 'TEST:B']
        np.testing.assert_array_almost_equal(
            test_b['split_ratio'].values, [1.0, 2.0, 1.0], decimal=6
        )

    def test_apply_corporate_actions_same_day_dividend_and_split(self):
        """Test corporate actions when dividend and split occur on the same day."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 4,
            'date_stamp': pd.date_range('2020-01-01', periods=4, freq='D'),
            'price': [100.0, 101.0, 51.0, 52.0]
        })
        # Both dividend and split on day 3
        dividend_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'unadjusted_value': [2.0]
        })
        split_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=split_df)
        tsp._apply_corporate_actions()

        # On day 3, both dividend and split occur
        # G_3 = s_3 * (P_3 + D_3) / P_2 = 2.0 * (51 + 2) / 101 = 106/101
        expected_return_day3 = 2.0 * (51.0 + 2.0) / 101.0
        np.testing.assert_almost_equal(
            tsp.prices_df.iloc[2]['total_return'],
            expected_return_day3,
            decimal=6
        )

    def test_apply_corporate_actions_multiple_dividends_same_day(self):
        """Test corporate actions with multiple dividends on the same day (aggregation)."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        # Two dividends on day 2 (should be aggregated)
        dividend_df = pd.DataFrame({
            'identity_code': ['TEST:A', 'TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02'), pd.Timestamp('2020-01-02')],
            'unadjusted_value': [1.0, 0.5]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=None)
        tsp._apply_corporate_actions()

        # Should aggregate to 1.5
        self.assertAlmostEqual(tsp.prices_df.iloc[1]['dividend'], 1.5, places=6)

    def test_apply_corporate_actions_reverse_split(self):
        """Test corporate actions with a reverse split (1-for-5)."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [10.0, 11.0, 55.0]  # Price jumps due to reverse split
        })
        # 1-for-5 reverse split on day 3
        split_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [1.0],
            'denominator': [5.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=split_df)
        tsp._apply_corporate_actions()

        # Check split_ratio = 1/5 = 0.2
        self.assertAlmostEqual(tsp.prices_df.iloc[2]['split_ratio'], 0.2, places=6)

        # Total return on day 3: 0.2 * 55 / 11 = 11/11 = 1.0
        self.assertAlmostEqual(tsp.prices_df.iloc[2]['total_return'], 1.0, places=6)

    def test_apply_corporate_actions_invalid_splits(self):
        """Test corporate actions with invalid split data (non-positive)."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        # Invalid split with zero denominator
        split_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'numerator': [2.0],
            'denominator': [0.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=split_df)

        with self.assertRaises(ValueError) as context:
            tsp._apply_corporate_actions()
        self.assertIn("non-positive split", str(context.exception))

    def test_apply_corporate_actions_lockout_after_downsampling(self):
        """Test that corporate actions cannot be applied after downsampling."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 5,
            'date_stamp': pd.date_range('2020-01-01', periods=5, freq='D'),
            'price': [100.0, 101.0, 102.0, 103.0, 104.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)

        # Simulate downsampling by setting the flag
        tsp.downsampled_total_returns_df = pd.DataFrame()

        with self.assertRaises(RuntimeError) as context:
            tsp._apply_corporate_actions()
        self.assertIn("Cannot apply corporate actions after downsampling", str(context.exception))

    def test_apply_corporate_actions_with_fixture_data(self):
        """Test corporate actions using the actual fixture data."""
        # Use the fixture data which has known dividends and splits
        # Note: fixture data contains NaN prices for weekends/holidays, which means
        # some trading days will have NaN prev_price and thus NaN total_return
        tsp = TimeSeriesProcessor(
            self.clean_test_price_df.copy(),
            self.test_dividend_df.copy(),
            self.test_split_df.copy()
        )
        tsp._apply_corporate_actions()

        # Verify that all expected columns exist
        expected_cols = ['dividend', 'split_ratio', 'total_return', 'prev_price']
        for col in expected_cols:
            self.assertIn(col, tsp.prices_df.columns)

        # Check that we have some valid total returns computed
        total_returns = tsp.prices_df['total_return']
        valid_returns = total_returns.dropna()
        self.assertGreater(len(valid_returns), 0, "Should have at least some valid total returns")

        # For rows where both price and prev_price are valid, total_return should be valid
        both_valid = tsp.prices_df[
            tsp.prices_df['price'].notna() & tsp.prices_df['prev_price'].notna()
        ]
        if len(both_valid) > 0:
            self.assertFalse(
                both_valid['total_return'].isna().any(),
                "Total return should be computed when both price and prev_price are valid"
            )

        # Check that dividends are applied on expected dates
        dividend_dates = tsp.prices_df[tsp.prices_df['dividend'] > 0]['date_stamp'].values
        # Filter test_dividend_df to only rows with actual dividend values
        expected_dividend_dates = self.test_dividend_df[
            self.test_dividend_df['unadjusted_value'].notna()
        ]['date_stamp'].values
        np.testing.assert_array_equal(dividend_dates, expected_dividend_dates)

        # Check that splits are applied on expected dates
        split_dates = tsp.prices_df[tsp.prices_df['split_ratio'] != 1.0]['date_stamp'].values
        # Filter test_split_df to only rows with actual split values
        expected_split_dates = self.test_split_df[
            (self.test_split_df['numerator'].notna()) &
            (self.test_split_df['denominator'].notna())
        ]['date_stamp'].values
        np.testing.assert_array_equal(split_dates, expected_split_dates)

    def test_get_total_return(self):
        """Test get_total_return method."""
        # Create simple price series with dividend and split
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 5,
            'date_stamp': pd.date_range('2020-01-01', periods=5, freq='D'),
            'price': [100.0, 101.0, 50.5, 51.0, 52.0]
        })
        dividend_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'unadjusted_value': [1.0]
        })
        split_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=split_df)

        # Should raise error before processing
        with self.assertRaises(RuntimeError) as context:
            tsp.get_total_return()
        self.assertIn("not yet computed", str(context.exception))

        # Apply corporate actions
        tsp._apply_corporate_actions()

        # Get total returns
        total_returns_df = tsp.get_total_return()

        # Verify structure
        self.assertIsInstance(total_returns_df, pd.DataFrame)
        expected_cols = ['identity_code', 'date_stamp', 'total_return']
        self.assertEqual(list(total_returns_df.columns), expected_cols)

        # Verify values
        self.assertEqual(len(total_returns_df), 5)
        self.assertTrue(pd.isna(total_returns_df['total_return'].iloc[0]))
        # Verify we have valid returns (actual values tested in corporate actions tests)
        self.assertGreater(total_returns_df['total_return'].dropna().count(), 0)

    def test_get_total_return_multiple_assets(self):
        """Test get_total_return with multiple assets."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A']*3 + ['TEST:B']*3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D').tolist() * 2,
            'price': [100.0, 102.0, 104.0, 200.0, 210.0, 220.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)
        tsp._apply_corporate_actions()
        total_returns_df = tsp.get_total_return()

        # Check both assets are present
        self.assertEqual(len(total_returns_df), 6)
        assets = total_returns_df['identity_code'].unique()
        self.assertEqual(len(assets), 2)
        self.assertIn('TEST:A', assets)
        self.assertIn('TEST:B', assets)

    def test_get_total_return_index(self):
        """Test get_total_return_index method."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 5,
            'date_stamp': pd.date_range('2020-01-01', periods=5, freq='D'),
            'price': [100.0, 100.0, 50.0, 51.0, 52.0]
        })
        dividend_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'unadjusted_value': [2.0]
        })
        split_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=split_df)

        # Should raise error before processing
        with self.assertRaises(RuntimeError) as context:
            tsp.get_total_return_index()
        self.assertIn("not yet computed", str(context.exception))

        # Apply corporate actions
        tsp._apply_corporate_actions()

        # Get TRI
        tri_df = tsp.get_total_return_index()

        # Verify structure
        self.assertIsInstance(tri_df, pd.DataFrame)
        expected_cols = ['identity_code', 'date_stamp', 'tri']
        self.assertEqual(list(tri_df.columns), expected_cols)

        # Verify TRI is cumulative product of returns
        self.assertEqual(len(tri_df), 5)

        # First TRI should be 1.0 (NaN return filled with 1.0)
        self.assertAlmostEqual(tri_df['tri'].iloc[0], 1.0, places=6)

        # Verify TRI values are positive and increasing
        tri_values = tri_df['tri'].values
        for i in range(len(tri_values)):
            self.assertGreater(tri_values[i], 0)

    def test_get_adjusted_price_anchor_first(self):
        """Test get_adjusted_price with anchor='first'."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 4,
            'date_stamp': pd.date_range('2020-01-01', periods=4, freq='D'),
            'price': [100.0, 100.0, 50.0, 51.0]  # 2-for-1 split on day 3
        })
        # 2-for-1 split on day 3
        split_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=split_df)

        # Should raise error before processing
        with self.assertRaises(RuntimeError) as context:
            tsp.get_adjusted_price()
        self.assertIn("not yet applied", str(context.exception))

        tsp._apply_corporate_actions()
        adj_price_df = tsp.get_adjusted_price(anchor='first')

        # Verify structure
        self.assertIsInstance(adj_price_df, pd.DataFrame)
        expected_cols = ['identity_code', 'date_stamp', 'adj_price']
        self.assertEqual(list(adj_price_df.columns), expected_cols)

        # With anchor='first', adjusted price should start at first raw price (100.0)
        self.assertAlmostEqual(adj_price_df['adj_price'].iloc[0], 100.0, places=6)

        # Verify adjusted prices are continuous (all positive values)
        self.assertTrue((adj_price_df['adj_price'] > 0).all())

    def test_get_adjusted_price_anchor_last(self):
        """Test get_adjusted_price with anchor='last'."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 4,
            'date_stamp': pd.date_range('2020-01-01', periods=4, freq='D'),
            'price': [100.0, 100.0, 50.0, 51.0]  # 2-for-1 split on day 3
        })
        # 2-for-1 split on day 3
        split_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=split_df)
        tsp._apply_corporate_actions()
        adj_price_df = tsp.get_adjusted_price(anchor='last')

        # With anchor='last', adjusted price should end at last raw price (51.0)
        self.assertAlmostEqual(adj_price_df['adj_price'].iloc[-1], 51.0, places=6)

    def test_get_adjusted_price_invalid_anchor(self):
        """Test get_adjusted_price with invalid anchor raises ValueError."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)
        tsp._apply_corporate_actions()

        with self.assertRaises(ValueError) as context:
            tsp.get_adjusted_price(anchor='invalid')
        self.assertIn("must be 'first' or 'last'", str(context.exception))

    def test_get_adjusted_price_multiple_assets(self):
        """Test get_adjusted_price with multiple assets."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A']*3 + ['TEST:B']*3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D').tolist() * 2,
            'price': [100.0, 102.0, 104.0, 200.0, 210.0, 220.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)
        tsp._apply_corporate_actions()
        adj_price_df = tsp.get_adjusted_price(anchor='first')

        # Check both assets are present
        self.assertEqual(len(adj_price_df), 6)
        assets = adj_price_df['identity_code'].unique()
        self.assertEqual(len(assets), 2)

        # Each asset should start at its own first price
        test_a = adj_price_df[adj_price_df['identity_code'] == 'TEST:A']
        test_b = adj_price_df[adj_price_df['identity_code'] == 'TEST:B']
        self.assertAlmostEqual(test_a['adj_price'].iloc[0], 100.0, places=6)
        self.assertAlmostEqual(test_b['adj_price'].iloc[0], 200.0, places=6)

    def test_get_downsampled_total_return(self):
        """Test get_downsampled_total_return method."""
        # Create daily prices for 2 weeks
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 14,
            'date_stamp': pd.date_range('2020-01-01', periods=14, freq='D'),
            'price': [100.0 + i for i in range(14)]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None, downsample_period_str='W')

        # Should raise error before processing
        with self.assertRaises(RuntimeError) as context:
            tsp.get_downsampled_total_return()
        self.assertIn("not yet computed", str(context.exception))

        tsp._apply_corporate_actions()
        downsampled_df = tsp.get_downsampled_total_return()

        # Verify structure
        self.assertIsInstance(downsampled_df, pd.DataFrame)
        expected_cols = {'identity_code', 'date_stamp', 'total_return'}
        self.assertEqual(set(downsampled_df.columns), expected_cols)

        # Should have fewer rows than original (weekly vs daily)
        self.assertLess(len(downsampled_df), 14)

        # Should have 2-3 weekly periods for 14 days
        self.assertGreaterEqual(len(downsampled_df), 2)
        self.assertLessEqual(len(downsampled_df), 3)

    def test_get_downsampled_total_return_with_corporate_actions(self):
        """Test downsampling with dividends and splits."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A'] * 14,
            'date_stamp': pd.date_range('2020-01-01', periods=14, freq='D'),
            'price': [100.0, 101.0, 102.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0, 60.0, 61.0]
        })
        # Dividend and split
        dividend_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'unadjusted_value': [2.0]
        })
        split_df = pd.DataFrame({
            'identity_code': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-04')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(
            price_df,
            dividends_df=dividend_df,
            splits_df=split_df,
            downsample_period_str='W'
        )
        tsp._apply_corporate_actions()
        downsampled_df = tsp.get_downsampled_total_return()

        # Verify downsampling produces valid results
        self.assertGreater(len(downsampled_df), 0)
        valid_returns = downsampled_df['total_return'].dropna()
        self.assertGreater(len(valid_returns), 0)

    def test_get_downsampled_total_return_multiple_assets(self):
        """Test downsampling with multiple assets."""
        price_df = pd.DataFrame({
            'identity_code': ['TEST:A']*14 + ['TEST:B']*14,
            'date_stamp': pd.date_range('2020-01-01', periods=14, freq='D').tolist() * 2,
            'price': [100.0 + i for i in range(14)] + [200.0 + i*2 for i in range(14)]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None, downsample_period_str='W')
        tsp._apply_corporate_actions()
        downsampled_df = tsp.get_downsampled_total_return()

        # Check both assets are present
        assets = downsampled_df['identity_code'].unique()
        self.assertEqual(len(assets), 2)
        self.assertIn('TEST:A', assets)
        self.assertIn('TEST:B', assets)

        # Each asset should have similar number of weekly observations
        test_a_count = len(downsampled_df[downsampled_df['identity_code'] == 'TEST:A'])
        test_b_count = len(downsampled_df[downsampled_df['identity_code'] == 'TEST:B'])
        self.assertEqual(test_a_count, test_b_count)
