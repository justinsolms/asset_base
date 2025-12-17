import numpy as np
import pandas as pd


class TimeSeriesProcessor():
    """ Clean and transform raw trade-daily price series into statistically
    usable return series.

    This class prepares raw price data for downstream return and risk analysis.
    Its primary purpose is to remove data artifacts that would otherwise bias
    estimates of statistical quantities such as mean returns, volatility, and
    cross-asset correlations.

    Processing steps
    ----------------
    The following operations are applied in a well-defined order:

    1. **Price validity checks**
    Ensures all prices are strictly positive. Non-positive prices are removed,
    as they invalidate geometric return calculations.

    2. **Sampling frequency validation**
    Ensures the input price series is sampled at *trade-daily* frequency.
    Lower-frequency inputs (e.g., weekly or monthly) are rejected with a
    ``ValueError``.

    3. **Date handling and ordering**
    - Ensures the ``date_stamp`` column is of pandas datetime type.
    - Sorts observations in ascending date order.
    - Removes duplicate dates, keeping the first occurrence.

    4. **Outlier removal**
    - Identify price transients such that outliers are removed whilst preserving
      genuine market jumps thereby preserving overall prices over time.
    - Return outliers are identified using a Z-score filter and replaced via
      linear interpolation.
    - Outliers are identified using z-scores. A typical outlier is identified
      when the z-score exceeds 3 standard deviations.

    4. **Missing data treatment**
    Missing price observations (e.g., due to holidays or incomplete source
    data) are filled using linear interpolation. Forward-filling is explicitly
    avoided, as it introduces artificial flat price segments followed by
    discrete jumps, which induce spurious auto- and cross-correlations in
    returns.
    After returns are computed, observations derived from interpolated prices
    are discarded to ensure that only returns based on observed market prices
    are retained.

    2. **Optional downsampling**
    If a resampling period is specified (e.g., weekly or monthly), prices are
    downsampled by taking the last available price in each period. This is
    equivalent to compounding geometric returns over the period and avoids
    distortions associated with arithmetic averaging.

    3. **Sample size adequacy check**
    Verifies that the final return series has sufficient observations for
    reliable estimation of the correlation matrix. As a rule of thumb, at
    least 10x as many observations as assets are required. Insufficient data
    results in a ``ValueError``.

    7. **Return computation**
    Computes geometric (log-compatible) returns from the cleaned price series.

    9. **Corporate action adjustments**
    - When dividend data are supplied, produces geometric *total return*
        series.
    - Otherwise, produces geometric price-only return series.
    - Applies share splits, when present, to adjust historical prices
        consistently.

    Parameters
    ----------
    prices_df : pandas.DataFrame
        Raw trade-daily price data with columns:
        ``['identity_code', 'date_stamp', 'price']``.

    dividends_df : pandas.DataFrame, optional
        Dividend data with columns:
        ``['identity_code', 'date_stamp', 'adjusted_value']``.
        If provided, total returns are computed. Default is ``None``.

    splits_df : pandas.DataFrame, optional
        Share split data with columns:
        ``['identity_code', 'date_stamp', 'numerator', 'denominator']``.
        Default is ``None``.

    downsample_period_str : str, optional
        Pandas resampling frequency string (e.g., ``'W'`` for weekly,
        ``'M'`` for monthly). An empty string disables downsampling.
        Default is ``""``.

    clean_outliers : bool, optional
        If ``True``, the processor will clean the data by removing outliers.
        Default is ``True``.

    The internal processing steps are executed upon each individual price series
    to maintain independence across series and avoid cross-asset contamination
    of data artifacts which cold arise when all series are processed jointly as
    columns of single DataFrame.

    Notes
    -----
    The ``identity_code`` column denotes a unique security identifier (e.g. ISIN).
    Depending on the database schema, this may also correspond to an ``isin``,
    an ``asset.Asset`` instance, or any other unique asset identifier used within
    the system.
    """

    def __init__(
        self,
        prices_df: pd.DataFrame,
        dividends_df: pd.DataFrame | None = None,
        splits_df: pd.DataFrame | None = None,
        downsample_period_str: str = "",
        clean_outliers: bool = True,
    ) -> None:
        """Initialize the TimeSeriesProcessor with type checking.

        Parameters
        ----------
        prices_df : pandas.DataFrame
            Raw trade-daily price data with required columns
            ``['identity_code', 'date_stamp', 'price']``.
        dividends_df : pandas.DataFrame | None
            Optional dividend data with columns ``['identity_code', 'date_stamp', 'adjusted_value']``.
        splits_df : pandas.DataFrame | None
            Optional split data with columns ``['identity_code', 'date_stamp', 'numerator', 'denominator']``.
        downsample_period_str : str
            Pandas resampling string (e.g. 'W', 'M'). Empty string disables downsampling.
        clean_outliers : bool
            Whether to run outlier cleaning steps.
        """

        # Basic type checks
        if not isinstance(prices_df, pd.DataFrame):
            raise TypeError("prices_df must be a pandas.DataFrame")

        required_price_cols = {"identity_code", "date_stamp", "price"}
        if not required_price_cols.issubset(set(prices_df.columns)):
            missing = required_price_cols - set(prices_df.columns)
            raise ValueError(f"prices_df is missing required columns: {missing}")

        if dividends_df is not None:
            if not isinstance(dividends_df, pd.DataFrame):
                raise TypeError("dividends_df must be a pandas.DataFrame or None")
            required_div_cols = {"identity_code", "date_stamp", "adjusted_value"}
            if not required_div_cols.issubset(set(dividends_df.columns)):
                missing = required_div_cols - set(dividends_df.columns)
                raise ValueError(f"dividends_df is missing required columns: {missing}")

        if splits_df is not None:
            if not isinstance(splits_df, pd.DataFrame):
                raise TypeError("splits_df must be a pandas.DataFrame or None")
            required_split_cols = {"identity_code", "date_stamp", "numerator", "denominator"}
            if not required_split_cols.issubset(set(splits_df.columns)):
                missing = required_split_cols - set(splits_df.columns)
                raise ValueError(f"splits_df is missing required columns: {missing}")

        if not isinstance(downsample_period_str, str):
            raise TypeError("downsample_period_str must be a str")

        if not isinstance(clean_outliers, bool):
            raise TypeError("clean_outliers must be a bool")

        # Assign attributes with explicit type hints
        self.prices_df: pd.DataFrame = prices_df.copy()
        self.dividends_df: pd.DataFrame | None = (
            dividends_df.copy() if dividends_df is not None else None
        )
        self.splits_df: pd.DataFrame | None = (
            splits_df.copy() if splits_df is not None else None
        )
        self.downsample_period_str: str = downsample_period_str
        self.clean_outliers: bool = clean_outliers

        # Normalize date column types where possible
        try:
            self.prices_df["date_stamp"] = pd.to_datetime(self.prices_df["date_stamp"])
        except Exception:
            raise TypeError("prices_df.date_stamp must be convertible to pandas datetime")

        if self.dividends_df is not None:
            try:
                self.dividends_df["date_stamp"] = pd.to_datetime(self.dividends_df["date_stamp"])
            except Exception:
                raise TypeError("dividends_df.date_stamp must be convertible to pandas datetime")

        if self.splits_df is not None:
            try:
                self.splits_df["date_stamp"] = pd.to_datetime(self.splits_df["date_stamp"])
            except Exception:
                raise TypeError("splits_df.date_stamp must be convertible to pandas datetime")

    def _validate_prices(self) -> None:
        """Validate that prices are strictly positive and numeric. """
        # Check for non-numeric prices
        if not pd.api.types.is_numeric_dtype(self.prices_df['price']):
            raise TypeError("prices_df.price must be numeric")

        # Remove non-positive prices
        invalid_prices = self.prices_df[self.prices_df['price'] <= 0]
        if not invalid_prices.empty:
            raise ValueError(
                f"Found non-positive prices in prices_df for identity_codes: "
                f"{invalid_prices['identity_code'].unique().tolist()}"
            )

    # Private processing step stubs (one per documented processing step)
    def _validate_sampling_frequency(self) -> None:
        """Validate that input price series is trade-daily frequency. """
        # Validate pandas.Dataframe sampling frequency for each identity_code
        # Group by identity_code and check sampling frequency for each group
        for identity_code, group in self.prices_df.groupby('identity_code'):
            # Sort by date to ensure proper frequency inference
            group = group.sort_values('date_stamp')

            # Infer the frequency of the date_stamp column
            inferred_freq = pd.infer_freq(group['date_stamp'])

            # Check if frequency is daily (business day 'B' or calendar day 'D')
            if inferred_freq not in ['B', 'D']:
                raise ValueError(
                    f"identity_code '{identity_code}' has non-daily sampling frequency: "
                    f"{inferred_freq}. Expected trade-daily ('B') or calendar-daily ('D') frequency."
                )

    def _normalize_and_order_dates(self) -> None:
        """Normalize `date_stamp` types, sort and deduplicate dates. """
        # Ensure date_stamp is datetime type
        self.prices_df['date_stamp'] = pd.to_datetime(self.prices_df['date_stamp'])

        # Sort by identity_code and date_stamp
        self.prices_df = self.prices_df.sort_values(by=['identity_code', 'date_stamp'])

        # Remove duplicate dates, keeping the first occurrence
        self.prices_df = self.prices_df.drop_duplicates(
            subset=['identity_code', 'date_stamp'], keep='first'
        )

    def _outlier_removal(self, deviation:float=3.0, iterations:int|None=None) -> None:
        """Remove outliers in prices and returns whilst preserving prices."""
        # Test for outliers and return an outlier index
        def test_for_outliers(price):
            diff = price.diff()
            # Zscore
            mean = diff['price'].mean()
            std = diff['price'].std()
            diff.iloc[0] = mean  # Guarantees firsts price won't be rejected.
            z_score = (group - mean) / std
            # Outliers identification and rejection
            outlier_index = z_score.abs() > deviation
            return outlier_index

        # Single pass price outlier removal
        def remove_price_outliers(price, outlier_index):
            price[outlier_index] = np.nan()  # Set outlier prices to NaN
            # Prices linear interpolation getting rid of NaNs.
            price.interpolate(method='linear')
            # Note that first price will never be a NaN but the sequence of last
            # prices could be if they were all outliers including the last price
            price = price.ffill()

        # Check for price outliers in each group
        for identity_code, group in self.prices_df.groupby('identity_code'):
            # Sort by date as proper sequence is critical
            group = group.sort_values('date_stamp')
            price = group['price']
            # Iterate until outliers are removed.
            outlier_index = test_for_outliers(price)
            while outlier_index.any() == True:
                # Remove outliers
                price = remove_price_outliers(price, outlier_index)
                # Test again for outliers
                outlier_index = test_for_outliers(price)
            self.prices_df[identity_code] = price

    def _apply_downsampling(self) -> None:
        """Step 2: Optionally downsample prices according to `downsample_period_str`.

        This method is a stub; when implemented it will modify `self.prices_df`.
        """
        pass

    def _check_sample_size_adequacy(self) -> None:
        """Step 3: Ensure sample size is adequate for downstream analysis.

        This method is a stub and should raise a ValueError when the sample
        size is insufficient.
        """
        pass

    def _handle_missing_data(self) -> None:
        """Step 6: Handle missing price observations (interpolation strategy).

        This method is a stub; when implemented it will perform interpolation
        and mark interpolated rows for removal after return computation.
        """
        pass

    def _compute_returns(self) -> pd.DataFrame:
        """Step 7: Compute geometric (log) returns from cleaned prices.

        Returns
        -------
        pandas.DataFrame
            DataFrame with columns ['identity_code', 'date_stamp', 'return'].
        """
        pass

    def _apply_corporate_actions(self) -> None:
        """Step 9: Apply dividends and splits to compute total returns.

        This method is a stub and should adjust historical prices/returns
        when dividends_df or splits_df are provided.
        """
        pass

    def get_returns(self) -> pd.DataFrame:
        """Get processed returns DataFrame.

        If `dividends_df` or `splits_df` are provided a total-return
        implementation would be used. For now this method delegates to
        `get_price_returns` and logs a warning when corporate-action
        adjustments are requested but not implemented.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing processed returns with columns:
            ['identity_code', 'date_stamp', 'return'].
        """
        # The implementation of returns processing goes here
        pass

    def get_price_returns(self):
        """Get the processed price returns DataFrame.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing processed price returns with columns:
            ['identity_code', 'date_stamp', 'return'].
        """
        # Implementation of price returns processing goes here
        pass



