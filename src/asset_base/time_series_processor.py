from operator import is_
from flask import g
import numpy as np
import pandas as pd

import scipy.stats as sp_stats

class TimeSeriesProcessor():
    """ Clean and transform raw trade-daily price series into statistically
    usable return series.

    This class prepares raw price data for downstream return and risk analysis.
    Its primary purpose is to normalize the data such that there would otherwise
    be biases in the estimates of statistical quantities such as mean returns,
    volatility, and cross-asset correlations.

    Parameters
    ----------
    prices_df : pandas.DataFrame
        Raw trade-daily price data with columns:
        ``['identity_code', 'date_stamp', 'price']``.
    dividends_df : pandas.DataFrame, optional
        Dividend data with columns:
        ``['identity_code', 'date_stamp', 'unadjusted_value']``.
        If provided, total returns are computed. Default is ``None``.
    splits_df : pandas.DataFrame, optional
        Share split data with columns:
        ``['identity_code', 'date_stamp', 'numerator', 'denominator']``.
        Default is ``None``.

    Note
    ----
    The 'identity_code' column is typically the ``asset.Asset.identity_code`` by
    convention, but any unique asset identifier may be used as long as it is
    consistent across prices, dividends, and splits DataFrames.

    Internal processing steps are executed upon each individual price series
    to maintain independence across series and avoid cross-asset contamination
    of data artifacts which cold arise when all series are processed jointly as
    columns of single DataFrame.

    Notes
    -----
    The ``identity_code`` column denotes a unique security identifier (e.g. ISIN).
    Depending on the database schema, this may also correspond to an ``isin``,
    an ``asset.Asset`` instance, or any other unique asset identifier used within
    the system.

    Processing steps
    ----------------
    The following operations are applied in a well-defined order:

    1. **Price validity checks**
    Ensures all prices are strictly positive. Non-positive prices are removed,
    as they invalidate geometric return calculations.

    1. **Sampling frequency validation**
    Ensures the input price series is sampled at *trade-daily* frequency.
    Lower-frequency inputs (e.g., weekly or monthly) are rejected with a
    ``ValueError``.

    1. **Date handling and ordering**
    - Ensures the ``date_stamp`` column is of pandas datetime type.
    - Sorts observations in ascending date order.
    - Removes duplicate dates, keeping the first occurrence.

    1. **Drop NaN prices**
    Removes any samples with NaN prices from the price DataFrame. Note that this
    step is performed after validating the sampling frequency to ensure that
    the frequency check is not affected by missing data,

    1. **Sample size adequacy check**
    Verifies that the each price series has sufficient observations for
    reliable estimation of the correlation matrix. As a rule of thumb, at
    least 10x as many observations as assets are required. Insufficient data
    results in a ``ValueError``. This check is performed after NaN removal to
    ensure that only valid data points are counted.

    1. **Corporate action adjustments**
    If dividend and/or split data are provided, total-return adjustments are
    applied to compute a total-return price series. This involves merging
    dividend and split data onto the price series, computing total return
    factors, and constructing a total return index (TRI). An adjusted price
    series is also computed, scaled to either the first or last raw price
    based on the ``adj_price_anchor`` setting.

    1. **Downsampling**
    If a resampling period is specified (e.g., weekly or monthly), prices are
    downsampled by taking the last available price in each period. This is
    equivalent to compounding geometric returns over the period and avoids
    distortions associated with arithmetic averaging.

    Outlier identification
    ----------------------
    Outliers are not removed automatically. Instead, they are identified and
    flagged in the output DataFrame for manual review. This is to avoid
    inadvertent removal of genuine market jumps which are critical to risk
    modelling. The outlier identification process is as follows:

    - Identify price transients such that outliers are removed whilst preserving
      genuine market jumps thereby preserving overall prices over time.
    - Return outliers are identified using a Z-score filter and replaced via
      linear interpolation.
    - Outliers are identified using z-scores. A typical outlier is identified
      when the z-score exceeds 3 standard deviations.

    No code has been developed for outlier removal, only identification.
    There are three completely different species that gets mixed up:

    +--------------+--------------------------+-----------+----------------+
    | Type         | Example                  | Remove?   | Why            |
    +==============+==========================+===========+================+
    | Data error   | Bad tick, 1000% jump,    | YES       | Not economic   |
    |              | missing split adj.       |           | reality        |
    +--------------+--------------------------+-----------+----------------+
    | Liquidity    | Microcap gap, stale then | Usually   | Tradability    |
    | artifact     | catch-up                 | NO        | risk           |
    +--------------+--------------------------+-----------+----------------+
    | Market shock | 2008, COVID, flash crash | NEVER     | Is the risk    |
    |              |                          |           | being modelled |
    +--------------+--------------------------+-----------+----------------+

    Most “outlier removal” accidentally deletes the third category — which is
    exactly something we wish to model. It would be best to identify outliers
    and judiciously remove only the first category by hand in, say, by entering
    them into a special outlier file TBDL.

    """

    def __init__(
        self,
        prices_df: pd.DataFrame,
        dividends_df: pd.DataFrame | None = None,
        splits_df: pd.DataFrame | None = None,
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
            required_div_cols = {"identity_code", "date_stamp", "unadjusted_value"}
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

        # Assign attributes with explicit type hints
        self.prices_df: pd.DataFrame = prices_df.copy()
        self.dividends_df: pd.DataFrame | None = (
            dividends_df.copy() if dividends_df is not None else None
        )
        self.splits_df: pd.DataFrame | None = (
            splits_df.copy() if splits_df is not None else None
        )
        self.downsampled_total_returns_df: pd.DataFrame | None = None

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

    def process(self) -> None:
        """Execute all processing steps in order.

        Steps:
        1. Validate prices
        2. Validate sampling frequency
        3. Normalize and order dates
        4. Drop NaN prices
        5. Check sample size adequacy
        6. Apply corporate actions

        """
        # Complete processing steps
        self._validate_prices()
        self._validate_sampling_frequency()
        self._normalize_and_order_dates()
        self._dropna_prices()
        self._check_sample_size_adequacy()
        self._apply_corporate_actions()

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

    def _check_sample_size_adequacy(self, min_samples_factor:int = 10) -> None:
        """Ensure sample size is adequate for downstream analysis.

        Check that there are enough observations for reliable correlation
        matrix estimation. A rule of thumb is at least 10x as many
        observations as assets.
        """
        num_assets = self.prices_df['identity_code'].nunique()

        # Check each series has enough observations
        for identity_code, group in self.prices_df.groupby('identity_code'):
            num_observations = group['date_stamp'].nunique()
            if num_observations < min_samples_factor * num_assets:
                raise ValueError(
                    f"Insufficient data for {identity_code}: "
                    f"{num_observations} observations for {num_assets} assets. "
                    f"At least {min_samples_factor} times {num_assets} "
                    "observations are required for reliable correlation estimation."
                )

    def _apply_corporate_actions(self) -> None:
        """Apply corporate actions (dividends & splits) to compute total returns.

        Notes / conventions
        -------------------
        - `self.prices_df['price']` is assumed to be the raw exchange-listed close.
        - Dividends are assumed to be *cash per share* with ex-date == date_stamp.
        (EODHD's "unadjusted" dividend is typically this.)
        - Splits are given as numerator/denominator and interpreted as:
            s_t = numerator / denominator  (new shares per old share)
        So a 2-for-1 split => s_t = 2/1 = 2, and a 1-for-5 reverse split => 1/5.

        The per-share wealth recursion from t-1 close to t close is:
            G_t = s_t * (P_t + D_t) / P_{t-1}
            R_t = G_t - 1

        This handles dividends and splits on the same day without any ordering rules.

        Adds the following columns to `self.prices_df`:
        - 'dividend': cash dividend per share on date_stamp
        - 'split_ratio': share split ratio on date_stamp
        - 'total_return': Net total return factor G_t
        - 'prev_price': prior day's raw close price P_{t-1} (for debugging)
        """

        # Lock out corporate action adjustments after downsampling
        if self.downsampled_total_returns_df is not None:
            raise RuntimeError(
                "Cannot apply corporate actions after downsampling. Corporate "
                "actions must be applied to original price series."
            )

        # Ensure canonical ordering
        self.prices_df = self.prices_df.sort_values(["identity_code", "date_stamp"]).copy()

        # -----------------------
        # Build dividend series
        # -----------------------
        if self.dividends_df is not None and not self.dividends_df.empty:
            div = self.dividends_df.copy()

            # Ensure expected column exists (constructor enforces it)
            # Aggregate in case multiple dividends on one date (rare but possible)
            div = (
                div.groupby(["identity_code", "date_stamp"], as_index=False)["unadjusted_value"]
                .sum()
                .rename(columns={"unadjusted_value": "dividend"})
            )
        else:
            div = None

        # -----------------------
        # Build split ratio series
        # -----------------------
        if self.splits_df is not None and not self.splits_df.empty:
            spl = self.splits_df.copy()

            # Basic validation / safety
            for col in ("numerator", "denominator"):
                if not pd.api.types.is_numeric_dtype(spl[col]):
                    raise TypeError(f"splits_df.{col} must be numeric")

            if (spl["numerator"] <= 0).any() or (spl["denominator"] <= 0).any():
                bad = spl[(spl["numerator"] <= 0) | (spl["denominator"] <= 0)][
                    ["identity_code", "date_stamp", "numerator", "denominator"]
                ]
                raise ValueError(
                    "Found non-positive split numerator/denominator. "
                    f"Bad rows:\n{bad.to_string(index=False)}"
                )

            spl["split_ratio"] = spl["numerator"] / spl["denominator"]

            # Aggregate in case multiple split-like events on same date:
            # multiply ratios (e.g., sequential actions posted same day)
            spl = (
                spl.groupby(["identity_code", "date_stamp"], as_index=False)["split_ratio"]
                .prod()
            )
        else:
            spl = None

        # -----------------------
        # Merge corporate actions onto prices
        # -----------------------
        df = self.prices_df.copy()

        if div is not None:
            df = df.merge(div, on=["identity_code", "date_stamp"], how="left")
        else:
            df["dividend"] = np.nan

        if spl is not None:
            df = df.merge(spl, on=["identity_code", "date_stamp"], how="left")
        else:
            df["split_ratio"] = np.nan

        # Defaults: no dividend => 0, no split => 1
        df["dividend"] = df["dividend"].fillna(0.0)
        df["split_ratio"] = df["split_ratio"].fillna(1.0)

        # -----------------------
        # Compute total return factor per identity_code
        # -----------------------
        out_groups = []
        for identity_code, group in df.groupby("identity_code", sort=False):
            group = group.sort_values("date_stamp").copy()

            # Prior close (raw)
            group["prev_price"] = group["price"].shift(1)

            # If prev_price missing (first obs), factor/return undefined
            # Protect against divide-by-zero just in case
            if (group["prev_price"] <= 0).any():
                bad = group[group["prev_price"] <= 0][["identity_code", "date_stamp", "prev_price"]]
                raise ValueError(
                    "Found non-positive prev_price after shifting (unexpected). "
                    f"Bad rows:\n{bad.to_string(index=False)}"
                )

            # Gross total return factor:
            #   G_t = s_t * (P_t + D_t) / P_{t-1}
            # Works whether or not a split and dividend occur on same day.
            group["total_return"] = np.where(
                group["prev_price"].notna(),
                (group["split_ratio"] * (group["price"] + group["dividend"])) / group["prev_price"],
                np.nan,
            )

            out_groups.append(group)

        df_out = pd.concat(out_groups, ignore_index=True)

        # NOTE: Drop helper column if you don't want it hanging around
        # (I keep it because it's useful for debugging and validation.)
        # df_out = df_out.drop(columns=["prev_price"])

        self.prices_df = df_out

    def get_date_index(self) -> pd.DatetimeIndex:
        """Get unique sorted date index across all identity_codes.

        Returns
        -------
        pandas.DatetimeIndex
            DatetimeIndex of unique sorted dates across all identity_codes.
        """
        date_index: pd.DatetimeIndex = pd.DatetimeIndex(
            sorted(self.prices_df['date_stamp'].unique())
        )
        return date_index

    def get_total_return(self) -> pd.DataFrame:
        """Get total return DataFrame.

        Returns
        -------
        pandas.DataFrame
            DataFrame with total returns with columns:
            ``['identity_code', 'date_stamp', 'total_return']``.
        """
        # Verify that the processing step has been applied
        if 'total_return' not in self.prices_df.columns:
            raise RuntimeError(
                "Total returns not yet computed. Please run process() to apply "
                "corporate actions first."
            )
        total_returns_df: pd.DataFrame = self.prices_df[
            ['identity_code', 'date_stamp', 'total_return']
        ].copy()
        return total_returns_df

    def get_total_return_index(self) -> pd.DataFrame:
        """Get total return index DataFrame.

        Returns
        -------
        pandas.DataFrame
            DataFrame with total return index with columns:
            ``['identity_code', 'date_stamp', 'tri']``.
        """
        # Verify that the processing step has been applied
        if 'total_return' not in self.prices_df.columns:
            raise RuntimeError(
                "Total returns not yet computed. Please run process() to apply "
                "corporate actions first."
            )
        group_list = []
        for identity_code, group in self.prices_df.groupby('identity_code'):
            group["tri"] = group["total_return"].fillna(1.0).cumprod()
            group_list.append(group)
        tri_df: pd.DataFrame = pd.concat(group_list, ignore_index=True)[
            ['identity_code', 'date_stamp', 'tri']
        ]
        return tri_df

    def get_adjusted_price(self, anchor="first") -> pd.DataFrame:
        """Get adjusted prices DataFrame.

        Returns
        -------
        pandas.DataFrame
            DataFrame with adjusted prices with columns:
            ``['identity_code', 'date_stamp', 'adj_price']``.
        """
        # Verify that the processing step has been applied
        if 'total_return' not in self.prices_df.columns:
            raise RuntimeError(
                "Corporate actions not yet applied. Please run process() to apply "
                "corporate actions first."
            )
        tri = self.get_total_return_index()

        group_list = []
        for identity_code, group in tri.groupby('identity_code'):
            if anchor == "first":
                first_price = self.prices_df[
                    self.prices_df['identity_code'] == identity_code
                ]['price'].iloc[0]
                group['adj_price'] = group['tri'] * first_price
            elif anchor == "last":
                last_price = self.prices_df[
                    self.prices_df['identity_code'] == identity_code
                ]['price'].iloc[-1]
                last_tri = group['tri'].iloc[-1]
                group['adj_price'] = group['tri'] * (last_price / last_tri)
            else:
                raise ValueError("anchor must be 'first' or 'last'")
            group_list.append(group)
        adj_prices_df: pd.DataFrame = pd.concat(group_list, ignore_index=True)[
            ['identity_code', 'date_stamp', 'adj_price']
        ]
        return adj_prices_df

    def get_downsampled_total_return(self, frequency: str = "W") -> None:
        """Downsample prices according to `downsample_period_str` if set.

        Returns
        -------
        pandas.DataFrame
            DataFrame with downsampled total returns with columns:
            ``['identity_code', 'date_stamp', 'total_return']``.

        Downsampling is performed by compounding total returns over the
        specified period. This avoids distortions associated with arithmetic
        averaging of prices.
        """
        # Verify that the processing step has been applied
        if 'total_return' not in self.prices_df.columns:
            raise RuntimeError(
                "Total returns not yet computed. Please run process() to apply "
                "corporate actions first."
            )

        # Type check frequency is a pandas frequency string
        if not isinstance(frequency, str):
            raise TypeError("frequency must be a pandas frequency string.")

        group_list = []
        for identity_code, group in self.prices_df.groupby('identity_code'):
            # Set date_stamp as index for resampling
            group = group.set_index('date_stamp')
            # Resample total_returns
            total_return = group[['total_return']]  # Double brackets to keep as DataFrame
            # Compound total returns over the downsample period
            downsampled = total_return.resample(frequency).prod()
            # Reset index to restore date_stamp as a column
            downsampled = downsampled.reset_index()
            downsampled['identity_code'] = identity_code  # Re-add identity_code column
            group_list.append(downsampled)

        downsampled_total_returns_df: pd.DataFrame = pd.concat(group_list, ignore_index=True)
        return downsampled_total_returns_df

    @staticmethod
    def pivot_dataframes(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """Pivot DataFrame by identity_code and date_stamp.

        Takes a DataFrame with 'identity_code' and 'date_stamp' columns plus
        additional data columns, and creates a separate pivoted DataFrame for
        each data column. The pivoted DataFrames have 'date_stamp' as the index,
        'identity_code' values as column labels, and the data column values
        as the cell values.

        This is useful for transforming long-format time series data (as produced
        by the get_* methods) into wide-format matrices suitable for correlation
        analysis, visualization, or other cross-sectional operations.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame containing at least 'identity_code' and 'date_stamp' columns,
            plus one or more additional data columns to pivot.

        Returns
        -------
        dict[str, pd.DataFrame]
            Dictionary mapping column names to pivoted DataFrames. Each pivoted
            DataFrame has 'date_stamp' as index, 'identity_code' values as columns,
            and the corresponding data column as values.

        Raises
        ------
        TypeError
            If df is not a pandas.DataFrame.
        ValueError
            If required columns are missing or no data columns are present.

        Examples
        --------
        >>> df = pd.DataFrame({
        ...     'identity_code': ['AAPL', 'AAPL', 'MSFT', 'MSFT'],
        ...     'date_stamp': pd.to_datetime(['2020-01-01', '2020-01-02',
        ...                                    '2020-01-01', '2020-01-02']),
        ...     'price': [100.0, 101.0, 200.0, 202.0],
        ...     'total_return': [1.0, 1.01, 1.0, 1.01]
        ... })
        >>> pivoted = TimeSeriesProcessor.pivot_dataframes(df)
        >>> pivoted['price']
        identity_code  AAPL   MSFT
        date_stamp
        2020-01-01     100.0  200.0
        2020-01-02     101.0  202.0
        """
        # Validate input
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas.DataFrame")

        required_cols = {'identity_code', 'date_stamp'}
        if not required_cols.issubset(set(df.columns)):
            missing = required_cols - set(df.columns)
            raise ValueError(f"DataFrame is missing required columns: {missing}")

        # Identify data columns (everything except identity_code and date_stamp)
        data_columns = [col for col in df.columns if col not in required_cols]

        if not data_columns:
            raise ValueError(
                "DataFrame must have at least one data column beyond "
                "'identity_code' and 'date_stamp'"
            )

        # Create a pivoted DataFrame for each data column
        pivoted_dfs = {}
        for col in data_columns:
            pivoted = df.pivot(
                index='date_stamp',
                columns='identity_code',
                values=col
            )
            pivoted_dfs[col] = pivoted

        return pivoted_dfs

    @staticmethod
    def concat(tsp_list: list['TimeSeriesProcessor']) -> "TimeSeriesProcessor":
        """Concatenate multiple TimeSeriesProcessor instances.

        Parameters
        ----------
        tsp_list : list of TimeSeriesProcessor
            List of TimeSeriesProcessor instances to concatenate.

        Returns
        -------
        TimeSeriesProcessor
            A new TimeSeriesProcessor instance containing the concatenated data.

        Raises
        ------
        TypeError
            If any item in tsp_list is not a TimeSeriesProcessor instance.
        """
        if not all(isinstance(tsp, TimeSeriesProcessor) for tsp in tsp_list):
            raise TypeError("All items in tsp_list must be TimeSeriesProcessor instances")

        combined_prices = pd.concat([tsp.prices_df for tsp in tsp_list], ignore_index=True)

        combined_dividends = None
        if any(tsp.dividends_df is not None for tsp in tsp_list):
            dividend_dfs = [
                tsp.dividends_df for tsp in tsp_list if tsp.dividends_df is not None
            ]
            combined_dividends = pd.concat(dividend_dfs, ignore_index=True)

        combined_splits = None
        if any(tsp.splits_df is not None for tsp in tsp_list):
            split_dfs = [
                tsp.splits_df for tsp in tsp_list if tsp.splits_df is not None
            ]
            combined_splits = pd.concat(split_dfs, ignore_index=True)

        return TimeSeriesProcessor(
            prices_df=combined_prices,
            dividends_df=combined_dividends,
            splits_df=combined_splits
        )


