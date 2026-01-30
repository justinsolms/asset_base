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

        # TODO: Make all fixtures a single CSV string for easier modification. Include test outcomes.
        # Price data fixture - days of trade data with holidays and anomalies In
        # prices spikes cause twin outliers on the spike day and on the
        # following day unless there are twin spikes. Twin spikes may not look
        # like twins until the holiday or missing data rows between the twins
        # are removed. Jumps cause a single outlier on the jump day.
        price_csv = (
            "identity_code,date_stamp,price,anomaly,anomaly_value,holiday,is_outlier\n"
            f"{cls.identity_code},2020-12-01,100.06775291892592,,,,False\n"
            f"{cls.identity_code},2020-12-02,100.18027275047513,,,,False\n"
            f"{cls.identity_code},2020-12-03,100.36668080142832,,,,False\n"
            f"{cls.identity_code},2020-12-04,100.2888441723542,spike,5.0,,True\n"
            f"{cls.identity_code},2020-12-05,,,,Saturday,False\n"
            f"{cls.identity_code},2020-12-06,,,,Sunday,False\n"
            f"{cls.identity_code},2020-12-07,100.9204749470223,,,,True\n"
            f"{cls.identity_code},2020-12-08,100.8890930062304,,,,False\n"
            f"{cls.identity_code},2020-12-09,101.17026591119323,spike,5.0,,True\n"
            f"{cls.identity_code},2020-12-10,101.50306928669244,,,,True\n"
            f"{cls.identity_code},2020-12-11,101.64092016956126,,,,False\n"
            f"{cls.identity_code},2020-12-12,,,,Saturday,False\n"
            f"{cls.identity_code},2020-12-13,,,,Sunday,False\n"
            f"{cls.identity_code},2020-12-14,101.40140945000331,spike,5.0,,True\n"
            f"{cls.identity_code},2020-12-15,101.2196398635974,,,,True\n"
            f"{cls.identity_code},2020-12-16,,,,Day of Reconciliation,False\n"
            f"{cls.identity_code},2020-12-17,101.50240243394468,,,,False\n"
            f"{cls.identity_code},2020-12-18,101.3845961566702,spike,5.0,,True\n"
            f"{cls.identity_code},2020-12-19,,,,Saturday,False\n"
            f"{cls.identity_code},2020-12-20,,,,Sunday,False\n"
            f"{cls.identity_code},2020-12-21,101.54155218803882,spike,5.0,,False\n"
            f"{cls.identity_code},2020-12-22,101.54389381383967,,,,True\n"
            f"{cls.identity_code},2020-12-23,101.80207427172614,,,,False\n"
            f"{cls.identity_code},2020-12-24,101.68601363993766,jump,10.0,,True\n"
            f"{cls.identity_code},2020-12-25,,,,Christmas Day,False\n"
            f"{cls.identity_code},2020-12-26,,,,Saturday,False\n"
            f"{cls.identity_code},2020-12-27,,,,Sunday,False\n"
            f"{cls.identity_code},2020-12-28,101.79644170411808,,,,False\n"
            f"{cls.identity_code},2020-12-29,101.75020193212782,,,,False\n"
            f"{cls.identity_code},2020-12-30,101.61770968391927,,,,False\n"
            f"{cls.identity_code},2020-12-31,101.85936152507813,,,,False\n"
            f"{cls.identity_code},2021-01-01,,,,New Year's Day,False\n"
            f"{cls.identity_code},2021-01-02,,,,Saturday,False\n"
            f"{cls.identity_code},2021-01-03,,,,Sunday,False\n"
            f"{cls.identity_code},2021-01-04,102.22817974400611,,,,False\n"
            f"{cls.identity_code},2021-01-05,102.07755345816649,,,,False\n"
            f"{cls.identity_code},2021-01-06,101.92389580707979,,,,False\n"
            f"{cls.identity_code},2021-01-07,101.39015304020404,,,,False\n"
            f"{cls.identity_code},2021-01-08,101.50328197121854,,,,False\n"
            f"{cls.identity_code},2021-01-09,,,,Saturday,False\n"
            f"{cls.identity_code},2021-01-10,,,,Sunday,False\n"
        )
        price_df = pd.read_csv(StringIO(price_csv))

        # Alternatively read from the Excel fixture file
        # TODO: Make the Excel fixture file a CSV file for git friendliness
        # TODO: Get rid of inline CSV fixtures once all tests are converted
        fixture_df = pd.read_excel("tests/fixtures/time_series_processor_price_fixture.xlsx")
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
        dividend_csv = (
            "identity_code,date_stamp,unadjusted_value\n"
            f"{cls.identity_code},2020-12-29,1.25\n"
            f"{cls.identity_code},2020-12-04,1.25\n"
        )
        dividend_df = pd.read_csv(StringIO(dividend_csv))

        # Alternatively use the Excel price fixture file
        dividend_df = fixture_df[['identity_code', 'date_stamp', 'dividend']]
        dividend_df.rename(columns={'dividend': 'unadjusted_value'}, inplace=True)
        dividend_df['date_stamp'] = pd.to_datetime(dividend_df['date_stamp'])
        cls.test_dividend_df = dividend_df

        # Split data fixture - 2 split events
        split_csv = (
            "identity_code,date_stamp,numerator,denominator\n"
            f"{cls.identity_code},2020-12-07,2.0,1.0\n"
            f"{cls.identity_code},2020-12-09,4.0,1.0\n"
        )
        split_df = pd.read_csv(StringIO(split_csv))

        # Alternatively use the Excel price fixture file
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

    def test_median_absolute_deviation(self):
        """Test median absolute deviation calculation of clean prices."""
        self.median_clean = 0.11312893101450072
        self.mad_clean = 0.3790849382605728
        # Drop NaNs first then identify outliers
        self.tsp_dirty._dropna_prices()
        # Test that there are no NaNs after dropping
        self.assertFalse(self.tsp_dirty.prices_df['price'].isna().any())
        # Calculate price differences
        price_diff = self.tsp_dirty.prices_df['price'].diff()
        # Test first diff should be a NaN
        self.assertTrue(np.isnan(price_diff.iloc[0]))
        # Test two more differences for correctness
        self.assertEqual(price_diff[1], 0.11251983154920708)
        self.assertEqual(price_diff[2], 0.18640805095319024)
        median, mad = TimeSeriesProcessor._median_absolute_price_deviation(price_diff)
        # Test median and MAD values
        self.assertEqual(median, self.median_clean)
        self.assertEqual(mad, self.mad_clean)

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
        self.assertEqual(mod_z_scores[3], 12.685902167407624)
        self.assertEqual(mod_z_scores[4], -11.821883973838979)

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

    def test_apply_downsampling(self):
        """Test downsampling method."""
        # Downsample dirty prices
        self.tsp_dirty._apply_downsampling()
        # Test that the resulting DataFrame has weekly frequency
        self.assertEqual(pd.infer_freq(self.tsp_dirty.prices_df['date_stamp']), 'W-SUN')
        # Test that there are no NaNs after downsampling
        self.assertFalse(self.tsp_dirty.prices_df['price'].isna().any())
        # Downsample clean prices
        self.tsp_clean._apply_downsampling()
        # Test that the resulting DataFrame has weekly frequency
        self.assertEqual(pd.infer_freq(self.tsp_clean.prices_df['date_stamp']), 'W-SUN')
        # Test that there are no NaNs after downsampling
        self.assertFalse(self.tsp_clean.prices_df['price'].isna().any())
        # Test second attempt to downsample fails.
        with self.assertRaises(RuntimeError):
            self.tsp_clean._apply_downsampling()

    def test_check_sample_size_adequacy(self):
        """Test sample size adequacy check method."""
        # Test with single asset and sufficient observations (should pass)
        self.tsp_clean._check_sample_size_adequacy(min_samples_factor=10)

        # Test with insufficient observations using the fixture (should raise ValueError)
        # The insufficient fixture has only 5 price observations for 1 asset
        with self.assertRaises(ValueError) as context:
            self.tsp_insufficient._check_sample_size_adequacy(min_samples_factor=10)
        self.assertIn("Insufficient data", str(context.exception))

    def test_apply_corporate_actions(self):
        """Test corporate actions application (dividends and splits)."""
        # Use clean prices and ensure they're normalized and ordered
        self.tsp_clean._dropna_prices()
        self.tsp_clean._normalize_and_order_dates()

        # Apply corporate actions
        self.tsp_clean._apply_corporate_actions()

        #  Set pandas to print all columns
        pd.set_option('display.max_columns', None)
        # Print the resulting prices DataFrame for visual inspection
        print()
        print(self.tsp_clean.prices_df)
        print(self.test_price_df)

        # Get result DataFrame
        result_df = self.tsp_clean.prices_df

        # Check that required columns exist
        required_cols = ['dividend', 'split_ratio', 'gross_factor', 'total_return', 'tri', 'adj_price']
        for col in required_cols:
            self.assertIn(col, result_df.columns, f"Missing column: {col}")

        # Check dividend values
        # According to fixtures, dividends occur on 2020-12-03 (1.25) and 2020-12-04 (1.25)
        div_date1 = pd.Timestamp('2020-12-29')
        div_date2 = pd.Timestamp('2021-01-06')

        div_row1 = result_df[result_df['date_stamp'] == div_date1].iloc[0]
        div_row2 = result_df[result_df['date_stamp'] == div_date2].iloc[0]

        self.assertEqual(div_row1['dividend'], 2.2, "Dividend on 2020-12-29 should be 2.2")
        self.assertEqual(div_row2['dividend'], 0.55, "Dividend on 2021-01-06 should be 0.55")

        # Check split values
        # According to fixtures, splits occur on 2020-12-07 (2:1) and 2020-12-09 (4:1)
        split_date1 = pd.Timestamp('2021-01-04')
        split_date2 = pd.Timestamp('2021-01-06')

        split_row1 = result_df[result_df['date_stamp'] == split_date1].iloc[0]
        split_row2 = result_df[result_df['date_stamp'] == split_date2].iloc[0]

        self.assertEqual(split_row1['split_ratio'], 2.0, "Split ratio on 2021-01-04 should be 2.0")
        self.assertEqual(split_row2['split_ratio'], 2.0, "Split ratio on 2021-01-06 should be 2.0")

        # Check gross factor calculation
        # First observation should have NaN gross_factor (no previous price)
        self.assertTrue(pd.isna(result_df.iloc[0]['gross_factor']),
                        "First observation should have NaN gross_factor")

        # For other observations, gross_factor should be positive and finite
        gross_factors = result_df['gross_factor'].dropna()
        self.assertTrue((gross_factors > 0).all(), "All gross factors should be positive")
        self.assertTrue(np.isfinite(gross_factors).all(), "All gross factors should be finite")

        # Check total return calculation
        # total_return = gross_factor - 1
        for idx in result_df.index[1:]:  # Skip first row
            if pd.notna(result_df.loc[idx, 'gross_factor']):
                expected_return = result_df.loc[idx, 'gross_factor'] - 1.0
                self.assertAlmostEqual(result_df.loc[idx, 'total_return'], expected_return, places=10,
                                       msg=f"Total return mismatch at index {idx}")

        # Check TRI (Total Return Index)
        tri = result_df['tri']
        self.assertTrue((tri > 0).all(), "All TRI values should be positive")
        self.assertTrue(np.isfinite(tri).all(), "All TRI values should be finite")
        # TRI should be monotonically increasing in general (can decrease with negative returns)
        self.assertGreater(tri.iloc[-1], 0, "Final TRI should be positive")

        # Check adjusted price with default anchor ('first')
        adj_price = result_df['adj_price']
        first_price = result_df.iloc[0]['price']
        first_adj_price = result_df.iloc[0]['adj_price']

        self.assertAlmostEqual(first_adj_price, first_price, places=10,
                               msg="With 'first' anchor, adj_price should equal raw price at start")

        # All adjusted prices should be positive and finite
        self.assertTrue((adj_price > 0).all(), "All adjusted prices should be positive")
        self.assertTrue(np.isfinite(adj_price).all(), "All adjusted prices should be finite")

        # Test with 'last' anchor
        tsp_last_anchor = TimeSeriesProcessor(
            self.clean_test_price_df.copy(),
            self.test_dividend_df.copy(),
            self.test_split_df.copy(),
            "",
            True,
            adj_price_anchor='last'
        )
        tsp_last_anchor._dropna_prices()
        tsp_last_anchor._normalize_and_order_dates()
        tsp_last_anchor._apply_corporate_actions()

        result_last = tsp_last_anchor.prices_df
        last_price = result_last.iloc[-1]['price']
        last_adj_price = result_last.iloc[-1]['adj_price']

        self.assertAlmostEqual(last_adj_price, last_price, places=10,
                               msg="With 'last' anchor, adj_price should equal raw price at end")

        # Test that corporate actions cannot be applied after downsampling
        tsp_downsample = TimeSeriesProcessor(
            self.clean_test_price_df.copy(),
            self.test_dividend_df.copy(),
            self.test_split_df.copy(),
            "W",  # Weekly downsampling
            True
        )
        tsp_downsample._dropna_prices()
        tsp_downsample._normalize_and_order_dates()
        tsp_downsample._apply_downsampling()

        with self.assertRaises(RuntimeError) as context:
            tsp_downsample._apply_corporate_actions()
        self.assertIn("Cannot apply corporate actions after downsampling", str(context.exception))

        # Test with no dividends
        tsp_no_div = TimeSeriesProcessor(
            self.clean_test_price_df.copy(),
            None,  # No dividends
            self.test_split_df.copy(),
            "",
            True
        )
        tsp_no_div._dropna_prices()
        tsp_no_div._normalize_and_order_dates()
        tsp_no_div._apply_corporate_actions()

        result_no_div = tsp_no_div.prices_df
        # All dividends should be 0 when no dividend data provided
        self.assertTrue((result_no_div['dividend'] == 0.0).all(),
                        "All dividends should be 0 when no dividend data provided")

        # Test with no splits
        tsp_no_split = TimeSeriesProcessor(
            self.clean_test_price_df.copy(),
            self.test_dividend_df.copy(),
            None,  # No splits
            "",
            True
        )
        tsp_no_split._dropna_prices()
        tsp_no_split._normalize_and_order_dates()
        tsp_no_split._apply_corporate_actions()

        result_no_split = tsp_no_split.prices_df
        # All split ratios should be 1.0 when no split data provided
        self.assertTrue((result_no_split['split_ratio'] == 1.0).all(),
                        "All split ratios should be 1.0 when no split data provided")


