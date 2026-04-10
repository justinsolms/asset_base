import unittest
import numpy as np
import pandas as pd
from io import StringIO

from asset_base.time_series_processor import TimeSeriesProcessor


class TestTimeSeriesProcessor(unittest.TestCase):
    """Test TimeSeriesProcessor functionality."""

    @classmethod
    def setUpClass(cls):
        """Set up class-wide test fixtures."""
        super().setUpClass()
        cls.identity_code = "XNYS:ABC"  # ABC Inc. asset code as example

        # Price data fixture - days of trade data with holidays and anomalies In
        # prices spikes cause twin outliers on the spike day and on the
        # following day unless there are twin spikes. Twin spikes may not look
        # like twins until the holiday or missing data rows between the twins
        # are removed. Jumps cause a single outlier on the jump day.
        fixture_df = pd.read_csv("tests/fixtures/time_series_processor_price_fixture.csv")
        price_df = fixture_df[['identity_code', 'date_stamp', 'price', 'anomaly', 'anomaly_value', 'holiday', 'is_outlier']]
        price_df = price_df.rename(columns={'identity_code': 'asset'})
        price_df['date_stamp'] = pd.to_datetime(price_df['date_stamp'])
        cls.test_price_df = price_df.copy()

        # Create clean price DataFrame without anomalies
        cls.clean_test_price_df = price_df[['asset', 'date_stamp', 'price']]

        # Create dirty price DataFrame with anomalies applied
        price_df = price_df.set_index(['asset', 'date_stamp'])
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
        cls.dirty_test_price_df = price_df[['asset', 'date_stamp', 'price']]

        # Create a tsp with insufficient rows to meet sample size adequacy
        # for multiple assets test later
        cls.insufficient_test_price_df = price_df[['asset', 'date_stamp', 'price']].iloc[:5]

        # Assert that days with holiday names are NaN in price as there would be
        # no trading on those days
        assert price_df.loc[price_df['holiday'].notna(), 'price'].isna().all(), "Holiday prices should be NaN."

        # Price holidays bool index series
        cls.test_holidays_sr = price_df['holiday'].isna()

        # Dividend data fixture - 2 dividend events
        dividend_df = fixture_df[['identity_code', 'date_stamp', 'dividend']]
        dividend_df = dividend_df.rename(columns={'identity_code': 'asset'})
        dividend_df.rename(columns={'dividend': 'unadjusted_value'}, inplace=True)
        dividend_df['date_stamp'] = pd.to_datetime(dividend_df['date_stamp'])
        cls.test_dividend_df = dividend_df

        # Split data fixture - 2 split events
        split_df = fixture_df[['identity_code', 'date_stamp', 'numerator', 'denominator']]
        split_df = split_df.rename(columns={'identity_code': 'asset'})
        split_df['date_stamp'] = pd.to_datetime(split_df['date_stamp'])
        cls.test_split_df = split_df

    def setUp(self):
        """Set up test fixtures for each test method."""
        super().setUp()
        # Create fresh copies of the test data for each test
        self.tsp_clean = TimeSeriesProcessor(
            self.clean_test_price_df.copy(),
            self.test_dividend_df.copy(),
            self.test_split_df.copy())
        self.tsp_dirty = TimeSeriesProcessor(
            self.dirty_test_price_df.copy(),
            self.test_dividend_df.copy(),
            self.test_split_df.copy())

        self.tsp_insufficient = TimeSeriesProcessor(
            self.insufficient_test_price_df.copy(),
            self.test_dividend_df.copy(),
            self.test_split_df.copy())

    def tearDown(self):
        """Clean up after each test method."""
        # Clear any instance variables
        pass
        super().tearDown()

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
            self.tsp_dirty._prices_df.reset_index(drop=True),
            self.dirty_test_price_df.reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            self.tsp_dirty._dividends_df.reset_index(drop=True),
            self.test_dividend_df.reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            self.tsp_dirty._splits_df.reset_index(drop=True),
            self.test_split_df.reset_index(drop=True)
        )

    def test_init_rejects_identity_code_only_columns(self):
        """Strict schema: constructor rejects legacy identity_code-only input."""
        legacy_price_df = pd.DataFrame({
            'identity_code': [self.identity_code] * 3,
            'date_stamp': pd.date_range(start='2021-01-01', periods=3),
            'price': [100.0, 101.0, 102.0],
        })

        with self.assertRaises(ValueError) as context:
            TimeSeriesProcessor(legacy_price_df)

        self.assertIn("missing required columns", str(context.exception))
        self.assertIn("asset", str(context.exception))

    def test_init_accepts_asset_column(self):
        """Preferred asset column is accepted."""
        price_df = pd.DataFrame({
            'asset': [self.identity_code] * 3,
            'date_stamp': pd.date_range(start='2021-01-01', periods=3),
            'price': [100.0, 101.0, 102.0],
        })

        tsp = TimeSeriesProcessor(price_df)

        self.assertIn('asset', tsp._prices_df.columns)

    def test_validate_prices(self):
        """Test price validation method."""
        # Validate price test fixtures
        self.tsp_dirty._validate_prices()

        # Validate small price fixture
        invalid_price_df = pd.DataFrame({
            'asset': [self.identity_code]*5,
            'date_stamp': pd.date_range(start='2021-01-01', periods=5),
            'price': [100.0, 50.0, 30.0, 200.0, np.nan]
        })
        self.tsp_dirty._prices_df = invalid_price_df
        self.tsp_dirty._validate_prices()

        # Test non-numeric prices
        invalid_price_df = pd.DataFrame({
            'asset': [self.identity_code]*5,
            'date_stamp': pd.date_range(start='2021-01-01', periods=5),
            'price': [100.0, 50.0, 'abc', 200.0, np.nan]
        })
        self.tsp_dirty._prices_df = invalid_price_df
        with self.assertRaises(TypeError):
            self.tsp_dirty._validate_prices()

        # Test negative prices
        invalid_price_df = pd.DataFrame({
            'asset': [self.identity_code]*5,
            'date_stamp': pd.date_range(start='2021-01-01', periods=5),
            'price': [100.0, -50.0, 30.0, 200.0, np.nan]
        })
        self.tsp_dirty._prices_df = invalid_price_df
        with self.assertRaises(ValueError):
            self.tsp_dirty._validate_prices()

    def test_validate_daily_sampling_frequency(self):
        """Test sampling frequency validation method with daily ("D") data."""
        # Create business daily prices
        self.tsp_dirty._validate_sampling_frequency()
        # Tests that the frequency is set to daily
        self.assertEqual(pd.infer_freq(self.tsp_dirty._prices_df['date_stamp']), 'D')

    def test_validate_business_daily_sampling_frequency(self):
        """Test sampling frequency validation method with business daily ("B"|"C") data."""
        # Create business daily prices by resampling.
        business_daily_df = self.clean_test_price_df.set_index('date_stamp').resample('B').last().reset_index()
        self.tsp_dirty._prices_df = business_daily_df
        # Validate business daily data
        self.tsp_dirty._validate_sampling_frequency()
        # Tests that the frequency is set to daily
        self.assertEqual(pd.infer_freq(self.tsp_dirty._prices_df['date_stamp']), 'B')

    def test_validate_too_high_sampling_frequency(self):
        """Test sampling frequency validation method with high-frequency data."""
        # Create hourly prices by upsampling fixture data with ffill
        hourly_price_df = self.clean_test_price_df.set_index('date_stamp').resample('h').ffill().reset_index()
        self.tsp_dirty._prices_df = hourly_price_df
        # Validate failure on high-frequency data
        with self.assertRaises(ValueError):
            self.tsp_dirty._validate_sampling_frequency()

    def test_validate_almost_daily_sampling_frequency(self):
        """Test sampling frequency validation method with almost daily data."""
        # Create almost daily prices by dropping fixture weekend and holiday by NaNs
        self.tsp_dirty._dropna_prices()
        self.tsp_dirty._validate_sampling_frequency()
        # Tests that the frequency is set to daily
        self.assertEqual(pd.infer_freq(self.tsp_dirty._prices_df['date_stamp']), None)

    def test_dropna_prices(self):
        """Test dropping NaN prices method."""
        # Count NaNs before dropping
        nan_count_before = self.tsp_dirty._prices_df['price'].isna().sum()
        self.tsp_dirty._dropna_prices()
        # Count NaNs after dropping
        nan_count_after = self.tsp_dirty._prices_df['price'].isna().sum()
        # Test that NaN count decreased
        self.assertLess(nan_count_after, nan_count_before)
        # Test that there are no NaNs after dropping
        self.assertFalse(self.tsp_dirty._prices_df['price'].isna().any())

    def test_normalize_and_order_dates(self):
        """Test date normalization and ordering method."""
        # Shuffle the price DataFrame by random sampling to test ordering
        shuffled_price_df = self.clean_test_price_df.sample(frac=1).reset_index(drop=True)
        self.tsp_dirty._prices_df = shuffled_price_df
        self.tsp_dirty._normalize_and_order_dates()
        pd.testing.assert_frame_equal(
            self.tsp_dirty._prices_df.reset_index(drop=True),
            self.clean_test_price_df.sort_values(by=['asset', 'date_stamp']).reset_index(drop=True)
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
        self.assertFalse(self.tsp_dirty._prices_df['price'].isna().any())
        # Calculate price differences
        price_diff = self.tsp_dirty._prices_df['price'].diff()
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
        self.assertFalse(self.tsp_dirty._prices_df['price'].isna().any())
        # Calculate modified z-scores
        prices = self.tsp_dirty._prices_df['price']
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
        known_outliers = clean_test_df.set_index(['asset', 'date_stamp'])['is_outlier'].reset_index()
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
        message = str(context.exception)
        self.assertTrue(
            "Insufficient data" in message
            or "No asset has sufficient data" in message
        )

    def test_apply_corporate_actions_no_dividends_no_splits(self):
        """Test corporate actions with no dividends and no splits."""
        # Create a simple price series without dividends or splits
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 5,
            'date_stamp': pd.date_range('2020-01-01', periods=5, freq='D'),
            'price': [100.0, 101.0, 102.0, 103.0, 104.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)
        tsp._apply_corporate_actions()

        # Should have dividend and split_ratio columns filled with defaults
        self.assertIn('dividend', tsp._prices_df.columns)
        self.assertIn('split_ratio', tsp._prices_df.columns)
        self.assertIn('total_return', tsp._prices_df.columns)

        # All dividends should be 0.0
        self.assertTrue((tsp._prices_df['dividend'] == 0.0).all())
        # All split_ratios should be 1.0
        self.assertTrue((tsp._prices_df['split_ratio'] == 1.0).all())

        # Total return should be simple price return (no dividends/splits)
        # G_t = 1.0 * (P_t + 0) / P_{t-1} = P_t / P_{t-1}
        expected_returns = [np.nan, 101.0/100.0, 102.0/101.0, 103.0/102.0, 104.0/103.0]
        np.testing.assert_array_almost_equal(
            tsp._prices_df['total_return'].values,
            expected_returns,
            decimal=6
        )

    def test_apply_corporate_actions_with_dividends_only(self):
        """Test corporate actions with dividends but no splits."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 5,
            'date_stamp': pd.date_range('2020-01-01', periods=5, freq='D'),
            'price': [100.0, 101.0, 102.0, 103.0, 104.0]
        })
        # Dividend of 2.0 on day 3 (2020-01-03)
        dividend_df = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'unadjusted_value': [2.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=None)
        tsp._apply_corporate_actions()

        # Check dividend column
        expected_divs = [0.0, 0.0, 2.0, 0.0, 0.0]
        np.testing.assert_array_almost_equal(
            tsp._prices_df['dividend'].values,
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
            tsp._prices_df['total_return'].values,
            expected_returns,
            decimal=6
        )

    def test_apply_corporate_actions_with_splits_only(self):
        """Test corporate actions with splits but no dividends."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 5,
            'date_stamp': pd.date_range('2020-01-01', periods=5, freq='D'),
            'price': [100.0, 101.0, 50.5, 51.0, 52.0]  # Split on day 3
        })
        # 2-for-1 split on day 3 (2020-01-03)
        split_df = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=split_df)
        tsp._apply_corporate_actions()

        # Check split_ratio column
        expected_splits = [1.0, 1.0, 2.0, 1.0, 1.0]
        np.testing.assert_array_almost_equal(
            tsp._prices_df['split_ratio'].values,
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
            tsp._prices_df['total_return'].values,
            expected_returns,
            decimal=6
        )

    def test_apply_corporate_actions_with_dividends_and_splits(self):
        """Test corporate actions with both dividends and splits."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 6,
            'date_stamp': pd.date_range('2020-01-01', periods=6, freq='D'),
            'price': [100.0, 101.0, 102.0, 51.0, 52.0, 53.0]
        })
        # Dividend of 1.0 on day 3
        dividend_df = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'unadjusted_value': [1.0]
        })
        # 2-for-1 split on day 4
        split_df = pd.DataFrame({
            'asset': ['TEST:A'],
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
            tsp._prices_df['dividend'].values, expected_divs, decimal=6
        )
        np.testing.assert_array_almost_equal(
            tsp._prices_df['split_ratio'].values, expected_splits, decimal=6
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
            tsp._prices_df['total_return'].values,
            expected_returns,
            decimal=6
        )

    def test_apply_corporate_actions_multiple_assets(self):
        """Test corporate actions with multiple assets."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A']*3 + ['TEST:B']*3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D').tolist() * 2,
            'price': [100.0, 101.0, 102.0, 200.0, 202.0, 204.0]
        })
        # Dividend for TEST:A only
        dividend_df = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'unadjusted_value': [1.0]
        })
        # Split for TEST:B only
        split_df = pd.DataFrame({
            'asset': ['TEST:B'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=split_df)
        tsp._apply_corporate_actions()

        # TEST:A should have dividend on day 2
        test_a = tsp._prices_df[tsp._prices_df['asset'] == 'TEST:A']
        np.testing.assert_array_almost_equal(
            test_a['dividend'].values, [0.0, 1.0, 0.0], decimal=6
        )

        # TEST:B should have split on day 2
        test_b = tsp._prices_df[tsp._prices_df['asset'] == 'TEST:B']
        np.testing.assert_array_almost_equal(
            test_b['split_ratio'].values, [1.0, 2.0, 1.0], decimal=6
        )

    def test_apply_corporate_actions_same_day_dividend_and_split(self):
        """Test corporate actions when dividend and split occur on the same day."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 4,
            'date_stamp': pd.date_range('2020-01-01', periods=4, freq='D'),
            'price': [100.0, 101.0, 51.0, 52.0]
        })
        # Both dividend and split on day 3
        dividend_df = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'unadjusted_value': [2.0]
        })
        split_df = pd.DataFrame({
            'asset': ['TEST:A'],
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
            tsp._prices_df.iloc[2]['total_return'],
            expected_return_day3,
            decimal=6
        )

    def test_apply_corporate_actions_multiple_dividends_same_day(self):
        """Test corporate actions with multiple dividends on the same day (aggregation)."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        # Two dividends on day 2 (should be aggregated)
        dividend_df = pd.DataFrame({
            'asset': ['TEST:A', 'TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02'), pd.Timestamp('2020-01-02')],
            'unadjusted_value': [1.0, 0.5]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=None)
        tsp._apply_corporate_actions()

        # Should aggregate to 1.5
        self.assertAlmostEqual(tsp._prices_df.iloc[1]['dividend'], 1.5, places=6)

    def test_apply_corporate_actions_reverse_split(self):
        """Test corporate actions with a reverse split (1-for-5)."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [10.0, 11.0, 55.0]  # Price jumps due to reverse split
        })
        # 1-for-5 reverse split on day 3
        split_df = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [1.0],
            'denominator': [5.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=split_df)
        tsp._apply_corporate_actions()

        # Check split_ratio = 1/5 = 0.2
        self.assertAlmostEqual(tsp._prices_df.iloc[2]['split_ratio'], 0.2, places=6)

        # Total return on day 3: 0.2 * 55 / 11 = 11/11 = 1.0
        self.assertAlmostEqual(tsp._prices_df.iloc[2]['total_return'], 1.0, places=6)

    def test_apply_corporate_actions_invalid_splits(self):
        """Test corporate actions with invalid split data (non-positive)."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        # Invalid split with zero denominator
        split_df = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'numerator': [2.0],
            'denominator': [0.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=split_df)

        with self.assertRaises(ValueError) as context:
            tsp._apply_corporate_actions()
        self.assertIn("non-positive split", str(context.exception))



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

        # Verify that all expected columns exist (prev_price is now dropped)
        expected_cols = ['dividend', 'split_ratio', 'total_return']
        for col in expected_cols:
            self.assertIn(col, tsp._prices_df.columns)

        # Check that we have some valid total returns computed
        total_returns = tsp._prices_df['total_return']
        valid_returns = total_returns.dropna()
        self.assertGreater(len(valid_returns), 0, "Should have at least some valid total returns")

        # Check that dividends are applied on expected dates
        dividend_dates = tsp._prices_df[tsp._prices_df['dividend'] > 0]['date_stamp'].values
        # Filter test_dividend_df to only rows with actual dividend values
        expected_dividend_dates = self.test_dividend_df[
            self.test_dividend_df['unadjusted_value'].notna()
        ]['date_stamp'].values
        np.testing.assert_array_equal(dividend_dates, expected_dividend_dates)

        # Check that splits are applied on expected dates
        split_dates = tsp._prices_df[tsp._prices_df['split_ratio'] != 1.0]['date_stamp'].values
        # Filter test_split_df to only rows with actual split values
        expected_split_dates = self.test_split_df[
            (self.test_split_df['numerator'].notna()) &
            (self.test_split_df['denominator'].notna())
        ]['date_stamp'].values
        np.testing.assert_array_equal(split_dates, expected_split_dates)

    def test_get_total_return(self):
        """Test get_raw_price_info_dataframe includes total_return after processing."""
        # Create price series with sufficient data (need at least 10 observations)
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 12,
            'date_stamp': pd.date_range('2020-01-01', periods=12, freq='D'),
            'price': [100.0 + i for i in range(12)]
        })
        dividend_df = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-05')],
            'unadjusted_value': [1.0]
        })
        split_df = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-08')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=dividend_df, splits_df=split_df)
        tsp.process()

        # Get the full dataframe
        result_df = tsp.get_raw_price_info_dataframe()

        # Verify total_return column exists
        self.assertIn('total_return', result_df.columns)
        self.assertIn('asset', result_df.columns)
        self.assertIn('date_stamp', result_df.columns)

        # Verify we have 12 rows
        self.assertEqual(len(result_df), 12)
    def test_get_total_return_multiple_assets(self):
        """Test get_total_return with multiple assets."""
        pass



    def test_get_adjusted_price_anchor_first(self):
        """Test that adj_price_first is computed correctly."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 12,
            'date_stamp': pd.date_range('2020-01-01', periods=12, freq='D'),
            'price': [100.0 + i for i in range(12)]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)
        tsp.process()

        result_df = tsp.get_raw_price_info_dataframe()

        # Verify adj_price_first column exists
        self.assertIn('adj_price_first', result_df.columns)

        # First adj_price should equal first raw price
        self.assertAlmostEqual(result_df.iloc[0]['adj_price_first'], 100.0, places=6)

    def test_get_adjusted_price_anchor_last(self):
        """Test that adj_price_last is computed correctly."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 12,
            'date_stamp': pd.date_range('2020-01-01', periods=12, freq='D'),
            'price': [100.0 + i for i in range(12)]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)
        tsp.process()

        result_df = tsp.get_raw_price_info_dataframe()

        # Verify adj_price_last column exists
        self.assertIn('adj_price_last', result_df.columns)

        # Last adj_price should equal last raw price
        self.assertAlmostEqual(result_df.iloc[-1]['adj_price_last'], 111.0, places=6)
        pass









    def test_pivot_dataframes(self):
        """Test get_pivoted_price_info_dataframes_dict method."""
        # Need at least 10*2=20 observations for 2 assets
        price_df = pd.DataFrame({
            'asset': ['AAPL'] * 25 + ['MSFT'] * 25,
            'date_stamp': pd.date_range('2020-01-01', periods=25, freq='D').tolist() * 2,
            'price': [100.0 + i for i in range(25)] + [200.0 + i*2 for i in range(25)]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)
        tsp.process()

        pivoted = tsp.get_pivoted_price_info_dataframes_dict()

        # Should be a dict
        self.assertIsInstance(pivoted, dict)

        # Should contain 'price' key at minimum
        self.assertIn('price', pivoted)

        # Price pivot should have correct shape
        price_pivot = pivoted['price']
        self.assertEqual(price_pivot.shape, (25, 2))  # 25 dates x 2 assets
    def test_pivot_dataframes_invalid_input(self):
        pass

    def test_pivot_dataframes_with_get_methods(self):
        """Test pivoting with processed data including corporate actions."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 12,
            'date_stamp': pd.date_range('2020-01-01', periods=12, freq='D'),
            'price': [100.0 + i for i in range(12)]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)
        tsp.process()

        pivoted = tsp.get_pivoted_price_info_dataframes_dict()

        # Should have multiple columns after processing
        self.assertGreater(len(pivoted), 1)

        # Should include total_return, tri, adj_price_first, adj_price_last
        self.assertIn('total_return', pivoted)
        self.assertIn('tri', pivoted)
        self.assertIn('adj_price_first', pivoted)
        self.assertIn('adj_price_last', pivoted)

    def test_concat_basic_two_processors(self):
        """Test basic concatenation of two TimeSeriesProcessor instances."""
        # Create first processor with TEST:A
        price_df_a = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        tsp_a = TimeSeriesProcessor(price_df_a, dividends_df=None, splits_df=None)

        # Create second processor with TEST:B
        price_df_b = pd.DataFrame({
            'asset': ['TEST:B'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [200.0, 202.0, 204.0]
        })
        tsp_b = TimeSeriesProcessor(price_df_b, dividends_df=None, splits_df=None)

        # Concatenate
        tsp_combined = TimeSeriesProcessor.concat([tsp_a, tsp_b])

        # Verify result
        self.assertIsInstance(tsp_combined, TimeSeriesProcessor)
        self.assertEqual(len(tsp_combined._prices_df), 6)
        self.assertEqual(tsp_combined._prices_df['asset'].nunique(), 2)
        self.assertIn('TEST:A', tsp_combined._prices_df['asset'].values)
        self.assertIn('TEST:B', tsp_combined._prices_df['asset'].values)

    def test_concat_with_dividends(self):
        """Test concatenation when dividends are present."""
        # First processor with dividend
        price_df_a = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        dividend_df_a = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'unadjusted_value': [1.0]
        })
        tsp_a = TimeSeriesProcessor(price_df_a, dividends_df=dividend_df_a, splits_df=None)

        # Second processor with dividend
        price_df_b = pd.DataFrame({
            'asset': ['TEST:B'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [200.0, 202.0, 204.0]
        })
        dividend_df_b = pd.DataFrame({
            'asset': ['TEST:B'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'unadjusted_value': [2.0]
        })
        tsp_b = TimeSeriesProcessor(price_df_b, dividends_df=dividend_df_b, splits_df=None)

        # Concatenate
        tsp_combined = TimeSeriesProcessor.concat([tsp_a, tsp_b])

        # Verify dividends are combined
        self.assertIsNotNone(tsp_combined._dividends_df)
        self.assertEqual(len(tsp_combined._dividends_df), 2)
        self.assertIn('TEST:A', tsp_combined._dividends_df['asset'].values)
        self.assertIn('TEST:B', tsp_combined._dividends_df['asset'].values)

    def test_concat_with_splits(self):
        """Test concatenation when splits are present."""
        # First processor with split
        price_df_a = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 50.5]
        })
        split_df_a = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp_a = TimeSeriesProcessor(price_df_a, dividends_df=None, splits_df=split_df_a)

        # Second processor with split
        price_df_b = pd.DataFrame({
            'asset': ['TEST:B'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [200.0, 202.0, 101.0]
        })
        split_df_b = pd.DataFrame({
            'asset': ['TEST:B'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp_b = TimeSeriesProcessor(price_df_b, dividends_df=None, splits_df=split_df_b)

        # Concatenate
        tsp_combined = TimeSeriesProcessor.concat([tsp_a, tsp_b])

        # Verify splits are combined
        self.assertIsNotNone(tsp_combined._splits_df)
        self.assertEqual(len(tsp_combined._splits_df), 2)
        self.assertIn('TEST:A', tsp_combined._splits_df['asset'].values)
        self.assertIn('TEST:B', tsp_combined._splits_df['asset'].values)

    def test_concat_mixed_dividends_and_splits(self):
        """Test concatenation with mixed dividend/split presence."""
        # First processor with dividend only
        price_df_a = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        dividend_df_a = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'unadjusted_value': [1.0]
        })
        tsp_a = TimeSeriesProcessor(price_df_a, dividends_df=dividend_df_a, splits_df=None)

        # Second processor with split only
        price_df_b = pd.DataFrame({
            'asset': ['TEST:B'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [200.0, 202.0, 101.0]
        })
        split_df_b = pd.DataFrame({
            'asset': ['TEST:B'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp_b = TimeSeriesProcessor(price_df_b, dividends_df=None, splits_df=split_df_b)

        # Third processor with neither
        price_df_c = pd.DataFrame({
            'asset': ['TEST:C'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [300.0, 303.0, 306.0]
        })
        tsp_c = TimeSeriesProcessor(price_df_c, dividends_df=None, splits_df=None)

        # Concatenate
        tsp_combined = TimeSeriesProcessor.concat([tsp_a, tsp_b, tsp_c])

        # Verify combined data
        self.assertEqual(len(tsp_combined._prices_df), 9)
        self.assertIsNotNone(tsp_combined._dividends_df)
        self.assertIsNotNone(tsp_combined._splits_df)
        self.assertEqual(len(tsp_combined._dividends_df), 1)
        self.assertEqual(len(tsp_combined._splits_df), 1)

    def test_concat_single_processor(self):
        """Test concatenation with a single processor."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)

        # Concatenate single processor
        tsp_combined = TimeSeriesProcessor.concat([tsp])

        # Should be equivalent to original
        self.assertIsInstance(tsp_combined, TimeSeriesProcessor)
        self.assertEqual(len(tsp_combined._prices_df), 3)
        pd.testing.assert_frame_equal(
            tsp_combined._prices_df.sort_values(['asset', 'date_stamp']).reset_index(drop=True),
            tsp._prices_df.sort_values(['asset', 'date_stamp']).reset_index(drop=True)
        )

    def test_concat_overlapping_assets(self):
        """Test concatenation with overlapping assets (same identity_code)."""
        # First processor with TEST:A on dates 1-3
        price_df_a = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        tsp_a = TimeSeriesProcessor(price_df_a, dividends_df=None, splits_df=None)

        # Second processor with TEST:A on dates 4-6
        price_df_b = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-04', periods=3, freq='D'),
            'price': [103.0, 104.0, 105.0]
        })
        tsp_b = TimeSeriesProcessor(price_df_b, dividends_df=None, splits_df=None)

        # Concatenate
        tsp_combined = TimeSeriesProcessor.concat([tsp_a, tsp_b])

        # Should have 6 rows for same asset
        self.assertEqual(len(tsp_combined._prices_df), 6)
        self.assertEqual(tsp_combined._prices_df['asset'].nunique(), 1)

        # Verify all dates are present
        test_a_data = tsp_combined._prices_df[tsp_combined._prices_df['asset'] == 'TEST:A']
        self.assertEqual(len(test_a_data), 6)

    def test_concat_invalid_type(self):
        """Test concatenation with invalid input type."""
        price_df = pd.DataFrame({
            'asset': ['TEST:A'] * 3,
            'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
            'price': [100.0, 101.0, 102.0]
        })
        tsp = TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None)

        # Try to concatenate with non-TimeSeriesProcessor
        with self.assertRaises(TypeError) as context:
            TimeSeriesProcessor.concat([tsp, "not a processor"])
        self.assertIn("must be TimeSeriesProcessor instances", str(context.exception))

    def test_concat_preserves_data_integrity(self):
        """Test that concatenation preserves all data correctly."""
        # Create two processors with full corporate actions
        price_df_a = pd.DataFrame({
            'asset': ['TEST:A'] * 4,
            'date_stamp': pd.date_range('2020-01-01', periods=4, freq='D'),
            'price': [100.0, 101.0, 102.0, 103.0]
        })
        dividend_df_a = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-02')],
            'unadjusted_value': [1.0]
        })
        split_df_a = pd.DataFrame({
            'asset': ['TEST:A'],
            'date_stamp': [pd.Timestamp('2020-01-03')],
            'numerator': [2.0],
            'denominator': [1.0]
        })
        tsp_a = TimeSeriesProcessor(price_df_a, dividends_df=dividend_df_a, splits_df=split_df_a)

        price_df_b = pd.DataFrame({
            'asset': ['TEST:B'] * 4,
            'date_stamp': pd.date_range('2020-01-01', periods=4, freq='D'),
            'price': [200.0, 202.0, 204.0, 206.0]
        })
        dividend_df_b = pd.DataFrame({
            'asset': ['TEST:B'],
            'date_stamp': [pd.Timestamp('2020-01-04')],
            'unadjusted_value': [2.0]
        })
        tsp_b = TimeSeriesProcessor(price_df_b, dividends_df=dividend_df_b, splits_df=None)

        # Concatenate
        tsp_combined = TimeSeriesProcessor.concat([tsp_a, tsp_b])

        # Verify prices
        self.assertEqual(len(tsp_combined._prices_df), 8)

        # Verify dividends
        self.assertEqual(len(tsp_combined._dividends_df), 2)
        self.assertEqual(tsp_combined._dividends_df['unadjusted_value'].sum(), 3.0)

        # Verify splits
        self.assertEqual(len(tsp_combined._splits_df), 1)
        self.assertAlmostEqual(
            tsp_combined._splits_df.iloc[0]['numerator'] / tsp_combined._splits_df.iloc[0]['denominator'],
            2.0,
            places=6
        )

    def test_concat_three_processors(self):
        """Test concatenation of three processors."""
        processors = []
        for i, code in enumerate(['TEST:A', 'TEST:B', 'TEST:C']):
            price_df = pd.DataFrame({
                'asset': [code] * 3,
                'date_stamp': pd.date_range('2020-01-01', periods=3, freq='D'),
                'price': [(i+1)*100.0, (i+1)*101.0, (i+1)*102.0]
            })
            processors.append(TimeSeriesProcessor(price_df, dividends_df=None, splits_df=None))

        # Concatenate all three
        tsp_combined = TimeSeriesProcessor.concat(processors)

        # Verify
        self.assertEqual(len(tsp_combined._prices_df), 9)
        self.assertEqual(tsp_combined._prices_df['asset'].nunique(), 3)
        for code in ['TEST:A', 'TEST:B', 'TEST:C']:
            self.assertIn(code, tsp_combined._prices_df['asset'].values)

