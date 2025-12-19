from flask import g
import numpy as np
import pandas as pd

import scipy.stats as sp_stats

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
        """Initialize the TimeSeriesProcessor with type checking."""

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

    # TODO: Implement all processing steps using the private method stubs below
    # TODO: _dropna_prices should be called before outlier detection
    # TODO: The order matters
    # TODO: Have a great holiday!


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
        """Validate price frequency is trade or daily then set to daily. """
        # Validate pandas.Dataframe sampling frequency for each identity_code
        # Group by identity_code and check sampling frequency for each group
        for identity_code, group in self.prices_df.groupby('identity_code'):
            # Sort by date to ensure proper frequency inference
            group = group.sort_values('date_stamp')

            # Infer frequency from date_stamp and process accordingly
            frequency = pd.infer_freq(group['date_stamp'])
            if frequency == 'D':
                pass
            elif frequency in ['B', 'C']:
                pass
            elif frequency is not None and pd.to_timedelta(frequency) < pd.Timedelta(days=1):
                raise ValueError(
                    f"Prices for identity_code {identity_code} are sampled at "
                    f"higher than daily frequency ({frequency}). Please downsample "
                    f"to daily frequency."
                )
            elif frequency is not None and pd.to_timedelta(frequency) > pd.Timedelta(days=1):
                raise ValueError(
                    f"Prices for identity_code {identity_code} are sampled at "
                    f"lower than daily frequency ({frequency}). Please provide "
                    f"daily frequency data."
                )
            elif frequency is None:
                # Here the time series may be almost daily and have some missing
                # days such as weekends and holidays. We check the median
                # difference between dates to infer frequency.
                # Find the median difference between consecutive dates as median
                # is more robust to outliers than mean
                date_diffs = group['date_stamp'].diff().dropna()
                if date_diffs.empty:
                    raise ValueError(
                        f"Insufficient data to determine sampling frequency for "
                        f"identity_code: {identity_code}"
                    )
                median_diff = date_diffs.median()
                # Check if median_diff corresponds to daily frequency
                if median_diff > pd.Timedelta(days=1):
                    raise ValueError(
                        f"Prices for identity_code {identity_code} are not "
                        f"sampled at daily frequency. Found median difference of "
                        f"{median_diff}. You may need longer price series or higher "
                        f"frequency data."
                    )
                pass

    def _dropna_prices(self) -> None:
        """Drop samples with NaN prices."""
        group_list = []
        for identity_code, group in self.prices_df.groupby('identity_code'):
            group = group.dropna(subset=['price'])
            # Do not use update as it will not remove rows from the main DataFrame.
            # Instead create a brand new DataFrame without NaNs and assign it back.
            group_list.append(group)
        self.prices_df = pd.concat(group_list, ignore_index=True)

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

    @staticmethod
    def _median_absolute_price_deviation(price_diff:pd.Series) -> tuple[float, float]:
        """Median Absolute Deviation (MAD) of price changes.

        Parameters
        ----------
        price_diff : pandas.Series
            Series of price differences.

        Returns
        -------
        float
            Median of price differences.
        float
            Adjusted Median Absolute Deviation of price differences scaled by
            1.4826 or (1/0.6745) to be comparable to standard deviation.
        """
        # Check that prices is a pandas Series
        if not isinstance(price_diff, pd.Series):
            raise TypeError("price_diff must be a pandas.Series")
        median = price_diff.median()
        mad = (price_diff - median).abs().median()
        return median, 1.4826 * mad  # Scale MAD to be comparable to std deviation

    @staticmethod
    def _modified_z_score(prices:pd.Series) -> pd.Series:
        """Compute modified Z-scores for price changes using Median/MAD.

        Parameters
        ----------
        prices : pandas.Series
            Series of prices.

        Returns
        -------
        modified_z_score : pandas.Series
            Series of modified Z-scores for price differences.

        Notes
        -----
        The modified Z-score is computed as:
            Z_i = (X_i - median) / MAD
        where MAD is the Median Absolute Deviation scaled by 1.4826 to be
        comparable to standard deviation.
        """
        # Check that prices is a pandas Series
        if not isinstance(prices, pd.Series):
            raise TypeError("prices must be a pandas.Series")
        price_diff = prices.diff()
        median, mad = TimeSeriesProcessor._median_absolute_price_deviation(price_diff)
        modified_z_score = (price_diff - median) / mad
        return modified_z_score

    def _identify_outliers(self, deviation:float=3.0) -> pd.DataFrame:
        """Identify outliers.

        Returns
        -------
        pandas.DataFrame
            DataFrame with an additional boolean column 'is_outlier' indicating
            whether each price is an outlier based on the modified Z-score.
        """
        # Check for price outliers in each group and create an outlier index
        group_list = []
        for identity_code, group in self.prices_df.groupby('identity_code'):
            # Sort by date as proper sequence is critical
            group = group.sort_values('date_stamp')
            prices = group['price'].copy()
            outlier_index = TimeSeriesProcessor._modified_z_score(prices).abs() > deviation
            group['is_outlier'] = outlier_index
            # Drop the price column as it's no longer needed
            group_list.append(group)
        return pd.concat(group_list, ignore_index=True)

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



