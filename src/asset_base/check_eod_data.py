# %%
# Imports
import requests
import pandas as pd
import random
import time
import click
import json
import os
from collections import Counter
from asset_base.financial_data import MetaData
from asset_base import get_tmp_path
from tqdm import tqdm

# TODO: Check for missing dividend events acording the the detected event cadence


# Get all JSE ETF tickers
def get_etf_tickers() -> list[str]:
    metadata = MetaData().get_etfs_meta()
    tickers = metadata[metadata.mic == "XJSE"]["ticker"]
    tickers = [f"{tick}.JSE" for tick in tickers]
    return tickers

def get_distributions_dict() -> dict[str, bool]:
    """Get a dictionary mapping JSE ETF tickers to their distributions bool value."""
    metadata = MetaData().get_etfs_meta()
    distributions = metadata[metadata.mic == "XJSE"][["ticker", "distributions"]]
    distributions = {f"{row['ticker']}.JSE": row['distributions'] for _, row in distributions.iterrows()}
    return distributions

def get_listed_status() -> dict[str, str]:
    """Get a dictionary mapping JSE ETF tickers to their listed status ('listed'|'delisted')."""
    metadata = MetaData().get_etfs_meta()
    listed_status = metadata[metadata.mic == "XJSE"][["ticker", "status"]]
    listed_status = {f"{row['ticker']}.JSE": row['status'] for _, row in listed_status.iterrows()}
    return listed_status
# %%

API_TOKEN = "6998160da02945.10414119"
BASE_URL = "https://eodhistoricaldata.com/api/{endpoint}/{ticker}"

def get_tmp_data_path(ticker, data_type):
    """Generate tmp file path for ticker data with format: {ticker}_{data_type}.check.csv"""
    tmp_dir = get_tmp_path()
    filename = f"{ticker}_{data_type}.check.csv"
    return os.path.join(tmp_dir, filename)

def save_data_to_tmp(ticker, data_type, data):
    """Save dataframe data to CSV file in tmp directory."""
    file_path = get_tmp_data_path(ticker, data_type)
    if isinstance(data, pd.DataFrame):
        data.to_csv(file_path, index=False)
    return file_path

def load_data_from_tmp(ticker, data_type):
    """Load dataframe data from CSV file in tmp directory, return None if not found."""
    file_path = get_tmp_data_path(ticker, data_type)
    if os.path.exists(file_path):
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            tqdm.write(f"Warning: Could not load cached data for {ticker} [{data_type}]: {e}")
            return None
    return None

def load_all_cached_data():
    """Load all cached data from tmp/ directory with .check.csv identifier.
    
    Returns a dictionary mapping (ticker, data_type) tuples to DataFrames.
    """
    tmp_dir = get_tmp_path()
    cached_data = {}
    
    if not os.path.exists(tmp_dir):
        return cached_data
    
    for filename in os.listdir(tmp_dir):
        if not filename.endswith('.check.csv'):
            continue
        
        # Parse filename: {ticker}_{data_type}.check.csv
        parts = filename.replace('.check.csv', '').rsplit('_', 1)
        if len(parts) == 2:
            ticker, data_type = parts
            try:
                filepath = os.path.join(tmp_dir, filename)
                df = pd.read_csv(filepath)
                cached_data[(ticker, data_type)] = df
            except Exception as e:
                tqdm.write(f"Warning: Could not load cache file {filename}: {e}")
    
    return cached_data

def cleanup_cache():
    """Remove all cached files with .check.csv identifier from tmp directory."""
    tmp_dir = get_tmp_path()
    
    if not os.path.exists(tmp_dir):
        tqdm.write("Cache directory does not exist.")
        return 0
    
    files_removed = 0
    for filename in os.listdir(tmp_dir):
        if filename.endswith('.check.csv'):
            filepath = os.path.join(tmp_dir, filename)
            try:
                os.remove(filepath)
                files_removed += 1
                tqdm.write(f"Removed: {filename}")
            except Exception as e:
                tqdm.write(f"Error removing {filename}: {e}")
    
    tqdm.write(f"Cleanup complete. Removed {files_removed} cache file(s).")
    return files_removed

def fetch_data(ticker, endpoint):
    """Fetch data, returning None on request failure and an empty frame for no data.
    
    Downloads data from the API.
    """
    params = {
        "api_token": API_TOKEN,
        "fmt": "json",
        "from": "1900-01-01",
        "to": "2026-11-23",
        "period": "d",
        "order": "a"
    }
    
    try:
        response = requests.get(BASE_URL.format(endpoint=endpoint, ticker=ticker), params=params)
        response.raise_for_status()
        data = response.json()
        # EODHD returns empty lists if no data is found for splits/divs
        df = pd.DataFrame(data) if data else pd.DataFrame()
        
        return df
    except requests.exceptions.RequestException as e:
        status = f" (HTTP {e.response.status_code})" if e.response is not None else ""
        tqdm.write(f"Error fetching {endpoint} for {ticker}: {type(e).__name__}{status}")
        return None

def check_zeros(df, columns, ticker, data_type, verbose=False):
    """Identify zeros, optionally printing each affected row.
    
    Returns list of tuples: (column_name, count, dates_list)
    """
    issues = []
    
    if df.empty:
        return issues
    
    for col in columns:
        if col in df.columns:
            # Convert to numeric just in case it's parsed as string
            df[col] = pd.to_numeric(df[col], errors='coerce')
            zero_rows = df[df[col] == 0]
            
            if not zero_rows.empty:
                dates = zero_rows['date'].tolist() if 'date' in zero_rows.columns else []
                issues.append((f"{col}", len(zero_rows), dates))
                if verbose:
                    tqdm.write(f"  [!] ZERO VALUE found in {ticker} [{data_type}] - Column: '{col}'")
                    for _, row in zero_rows.iterrows():
                        tqdm.write(f"      Date: {row.get('date', 'Unknown')} | Value: 0")
    
    return issues

def check_scaling(df, column, ticker, data_type, verbose=False):
    """Identifies massive jumps (approx x100 or /100) between consecutive days.
    
    Returns list of tuples: (column_name, count, details_list)
    """
    issues = []
    
    if df.empty or column not in df.columns or len(df) < 2:
        return issues

    # Convert to numeric, coercing errors so we can do math
    series = pd.to_numeric(df[column], errors='coerce')
    
    # Calculate the ratio between the current row and the previous row
    ratios = series / series.shift(1)
    
    # Flag ratios greater than 90 (x100 jump) or less than 0.012 (/100 drop)
    # We use 90 and 0.012 to account for natural market movement on the day of the bad tick
    anomalies = df[(ratios > 90) | (ratios < 0.012)]
    
    if not anomalies.empty:
        details = []
        if verbose:
            tqdm.write(f"  [!] SCALING ISSUE suspected in {ticker} [{data_type}] - Column: '{column}'")
        for idx in anomalies.index:
            # Get position in series to safely access previous value
            idx_pos = series.index.get_loc(idx)
            prev_val = series.iloc[idx_pos - 1]
            curr_val = series.iloc[idx_pos]
            date = df.loc[idx, 'date']
            ratio = ratios.loc[idx]
            
            direction = "x100 Jump" if ratio > 1 else "/100 Drop"
            details.append((date, direction, prev_val, curr_val))
            if verbose:
                tqdm.write(f"      Date: {date} | {direction} | Prev: {prev_val} -> New: {curr_val}")
        
        issues.append((column, len(anomalies), details))
    
    return issues

def check_duplicate_dividends(df, ticker, verbose=False):
    """Identify duplicate dividend events on the same date.
    
    Returns list of tuples: (date, count, values_list) for dates with duplicates.
    """
    issues = []
    
    if df.empty or 'date' not in df.columns:
        return issues
    
    # Group by date and find duplicates
    date_counts = df['date'].value_counts()
    duplicate_dates = date_counts[date_counts > 1]
    
    if not duplicate_dates.empty:
        if verbose:
            tqdm.write(f"  [!] DUPLICATE DIVIDEND EVENTS found in {ticker}")
        for date in duplicate_dates.index:
            duplicates = df[df['date'] == date]
            count = len(duplicates)
            values = duplicates['value'].tolist() if 'value' in duplicates.columns else []
            issues.append((date, count, values))
            if verbose:
                tqdm.write(f"      Date: {date} | Count: {count} | Values: {values}")
    
    return issues

def count_issues_by_data_type(issue_groups):
    counts = Counter()
    for data_type, issues in issue_groups:
        counts[data_type] += sum(count for _, count, _ in issues)
    return counts

def format_data_type_counts(counts):
    return ", ".join(f"{data_type}: {count}" for data_type, count in counts.items() if count)

def format_issue_categories(ticker_issues):
    """Format issue categories for summary report.
    
    Returns a string like: "ZEROS(EOD: 500) | SCALING(DIV: 2)" or "CLEAN".
    """
    categories = []
    
    if ticker_issues['zero_values']:
        zero_counts = count_issues_by_data_type(ticker_issues['zero_values'])
        if sum(zero_counts.values()) > 0:
            categories.append(f"ZEROS({format_data_type_counts(zero_counts)})")
    
    if ticker_issues['scaling_issues']:
        scaling_counts = count_issues_by_data_type(ticker_issues['scaling_issues'])
        if sum(scaling_counts.values()) > 0:
            categories.append(f"SCALING({format_data_type_counts(scaling_counts)})")
    
    if ticker_issues['duplicate_dividends']:
        dup_counts = count_issues_by_data_type(ticker_issues['duplicate_dividends'])
        if sum(dup_counts.values()) > 0:
            categories.append(f"DUP DIV({format_data_type_counts(dup_counts)})")

    if ticker_issues['missing_data']:
        categories.append(f"NO DATA({', '.join(ticker_issues['missing_data'])})")

    if ticker_issues['failed_data']:
        categories.append(f"FETCH FAILED({', '.join(ticker_issues['failed_data'])})")

    if ticker_issues['skipped_data']:
        categories.append(f"SKIPPED({', '.join(ticker_issues['skipped_data'])})")
    
    if not categories:
        return "✅ CLEAN"
    
    return " | ".join(categories)

def print_summary_report(results, no_volume=False, no_zeros=False, no_clean=False):
    """Print summary report with one line per ticker, partitioned by listing status."""
    print("\n" + "=" * 80)
    print("SUMMARY REPORT")
    print("=" * 80)
    skipped_checks = []
    if no_zeros:
        skipped_checks.append("zero-value checks")
    if no_volume:
        skipped_checks.append("EOD volume")
    if skipped_checks:
        print(f"Checks skipped: {', '.join(skipped_checks)}")
    
    # Get listed status dictionary
    listed_status = get_listed_status()
    
    # Partition tickers by status
    tickers_by_status = {"listed": [], "delisted": [], "unknown": []}
    
    for ticker in sorted(results.keys()):
        status = listed_status.get(ticker, "unknown")
        if not isinstance(status, str) or status not in ("listed", "delisted"):
            status = "unknown"
        tickers_by_status[status].append(ticker)
    
    # Track overall statistics
    total_tickers = len(results)
    tickers_with_issues = 0
    zero_counts = Counter({"EOD": 0, "DIV": 0, "SPLIT": 0})
    scaling_counts = Counter({"EOD": 0, "DIV": 0, "SPLIT": 0})
    duplicate_counts = Counter({"DIV": 0})
    
    for listing_status, tickers in tickers_by_status.items():
        if not tickers:
            continue
        heading = f"{listing_status.upper()} TICKERS"
        print(f"\n{heading:<15} {'Issues':<50} {'Status'}")
        print("-" * 80)
        
        for ticker in tickers:
            ticker_issues = results[ticker]
            issue_summary = format_issue_categories(ticker_issues)
            
            # Count for overall statistics
            if ticker_issues['zero_values'] or ticker_issues['scaling_issues'] or ticker_issues['duplicate_dividends']:
                tickers_with_issues += 1
                if ticker_issues['zero_values']:
                    zero_counts.update(count_issues_by_data_type(ticker_issues['zero_values']))
                if ticker_issues['scaling_issues']:
                    scaling_counts.update(count_issues_by_data_type(ticker_issues['scaling_issues']))
                if ticker_issues['duplicate_dividends']:
                    duplicate_counts.update(count_issues_by_data_type(ticker_issues['duplicate_dividends']))
            if ticker_issues['failed_data'] or 'EOD' in ticker_issues['missing_data']:
                status = "INCOMPLETE"
            elif ticker_issues['zero_values'] or ticker_issues['scaling_issues'] or ticker_issues['duplicate_dividends']:
                status = "⚠️"
            else:
                status = "✅"
            
            has_data_issues = any((
                ticker_issues['zero_values'],
                ticker_issues['scaling_issues'],
                ticker_issues['duplicate_dividends'],
                ticker_issues['missing_data'],
                ticker_issues['failed_data'],
            ))
            if not no_clean or has_data_issues:
                print(f"{ticker:<15} {issue_summary:<50}  {status}")
    
    print("=" * 80)
    print(f"Total Tickers Checked: {total_tickers}")
    print(f"  Listed: {len(tickers_by_status['listed'])}")
    print(f"  Delisted: {len(tickers_by_status['delisted'])}")
    if tickers_by_status["unknown"]:
        print(f"  Unknown: {len(tickers_by_status['unknown'])}")
    print(f"Tickers with Issues: {tickers_with_issues}")
    zero_total = "SKIPPED" if no_zeros else sum(zero_counts.values())
    print(f"Total Zero Value Issues: {zero_total}")
    if sum(zero_counts.values()):
        print(f"Zero Value Issues by Data Type: {format_data_type_counts(zero_counts)}")
    print(f"Total Scaling Issues: {sum(scaling_counts.values())}")
    if sum(scaling_counts.values()):
        print(f"Scaling Issues by Data Type: {format_data_type_counts(scaling_counts)}")
    print(f"Total Duplicate Dividend Events: {sum(duplicate_counts.values())}")
    if sum(duplicate_counts.values()):
        print(f"Duplicate Dividend Events by Data Type: {format_data_type_counts(duplicate_counts)}")
    print("=" * 80)


def main(verb=0, no_eod=False, no_div=False, no_splits=False, test=False,
         no_volume=False, no_zeros=False, tickers=None, no_clean=False,
         cleanup=False, use_cache=False, listed=False, delisted=False):
    # Handle cleanup and exit
    if cleanup:
        cleanup_cache()
        return
    if listed and delisted:
        raise click.UsageError("--listed and --delisted cannot be used together.")
    
    # Track all results for summary
    results = {}
    tickers = list(get_etf_tickers() if tickers is None else tickers)
    if listed or delisted:
        wanted_status = "listed" if listed else "delisted"
        statuses = get_listed_status()
        tickers = [ticker for ticker in tickers if statuses.get(ticker) == wanted_status]
    if test:
        random.shuffle(tickers)
        tickers = tickers[:10]
    distributions = get_distributions_dict() if not no_div else {}
    
    # Load cached data only when explicitly requested.
    cached_data = load_all_cached_data() if use_cache else {}
    if cached_data:
        tqdm.write(f"Loaded {len(cached_data)} cached data file(s) from tmp directory.")
    
    eod_columns = ['open', 'high', 'low', 'close', 'adjusted_close']
    if not no_volume:
        eod_columns.append('volume')
    checks = [
        ("eod", "EOD", no_eod, eod_columns, 'close'),
        ("div", "DIV", no_div, ['value', 'unadjustedValue'], 'value'),
        ("splits", "SPLIT", no_splits, ['split'], None),
    ]
    
    for ticker in tqdm(tickers, desc="Auditing tickers", unit="ticker"):
        ticker_issues = {
            'zero_values': [],
            'scaling_issues': [],
            'duplicate_dividends': [],
            'missing_data': [],
            'failed_data': [],
            'skipped_data': []
        }
        
        if verb:
            tqdm.write(f"\n--- Checking {ticker} ---")

        for endpoint, data_type, skip, columns, scaling_column in checks:
            if skip:
                ticker_issues['skipped_data'].append(data_type)
                continue
            if endpoint == "div":
                expected = distributions.get(ticker)
                if expected is False:
                    ticker_issues['skipped_data'].append("DIV: no distributions expected")
                    continue
                if expected is not True:
                    tqdm.write(f"Warning: Unknown distribution expectation for {ticker}; checking DIV data.")
            
            # Try the cache first only when explicitly enabled.
            df = cached_data.get((ticker, endpoint)) if use_cache else None
            if df is None:
                df = fetch_data(ticker, endpoint)
                if df is not None and not df.empty:
                    save_data_to_tmp(ticker, endpoint, df)
            
            if df is None:
                ticker_issues['failed_data'].append(data_type)
            elif df.empty:
                ticker_issues['missing_data'].append(data_type)
                if verb:
                    tqdm.write(f"  [-] No {data_type} data found for {ticker}")
            else:
                display_type = {"EOD": "EOD", "DIV": "DIVIDENDS", "SPLIT": "SPLITS"}[data_type]
                if not no_zeros:
                    zero_issues = check_zeros(df, columns, ticker, display_type, verbose=bool(verb))
                    if zero_issues:
                        ticker_issues['zero_values'].append((data_type, zero_issues))
                if scaling_column:
                    scaling_issues = check_scaling(df, scaling_column, ticker, display_type, verbose=bool(verb))
                    if scaling_issues:
                        ticker_issues['scaling_issues'].append((data_type, scaling_issues))
                if endpoint == "div":
                    dup_issues = check_duplicate_dividends(df, ticker, verbose=bool(verb))
                    if dup_issues:
                        ticker_issues['duplicate_dividends'].append((data_type, dup_issues))
        
        results[ticker] = ticker_issues
            
        # Be polite to the API rate limits (EODHD allows ~1000 requests/minute, but good practice)
        time.sleep(0.1)
    
    # Print summary report
    print_summary_report(results, no_volume=no_volume, no_zeros=no_zeros, no_clean=no_clean)


@click.command()
@click.option("--verb", type=click.IntRange(0, 1), default=0, show_default=True,
              help="Verbosity: 0 for summary only, 1 for individual findings.")
@click.option("--no-eod", is_flag=True, help="Skip the EOD price and volume checks.")
@click.option("--no-div", is_flag=True, help="Skip the dividend checks.")
@click.option("--no-splits", is_flag=True, help="Skip the split checks.")
@click.option("--no-volume", is_flag=True, help="Exclude EOD volume from checks.")
@click.option("--no-zeros", is_flag=True, help="Skip zero-value checks for all data types.")
@click.option("--no-clean", is_flag=True, help="Hide clean tickers from the summary report.")
@click.option("--cleanup", is_flag=True, help="Remove all cached data files (.check.csv) from tmp directory and exit.")
@click.option("--cache", "use_cache", is_flag=True,
              help="Prefer cached data over API responses when available; fetched data is always cached.")
@click.option("--listed", is_flag=True, help="Audit only listed ETFs.")
@click.option("--delisted", is_flag=True, help="Audit only delisted ETFs.")
@click.option("--ticker", multiple=True, metavar="SYMBOL",
              help="Audit this ticker without the .JSE suffix; repeat for multiple tickers.")
@click.option("--test", is_flag=True, help="Audit a random selection of up to 10 selected tickers.")
def cli(verb, no_eod, no_div, no_splits, no_volume, no_zeros, no_clean, cleanup,
        use_cache, listed, delisted, ticker, test):
    """Audit configured JSE ETFs for missing, zero, scaled, or duplicate data.

    By default, the audit checks EOD prices and volume, dividends, splits, and
    zero values.  API responses are cached as ``.check.csv`` files in the
    package temporary directory.  Pass --cache to prefer those files for an
    existing audit, or --cleanup to remove all cache files and exit.

    Use --listed or --delisted to restrict the audit by listing status.  The
    two filters are mutually exclusive.  Use --ticker repeatedly for an explicit
    ticker list, optionally with --test to sample at most ten selections.

    \b
    Examples:
      python notebooks/check_eod_data.py
      python notebooks/check_eod_data.py --listed --verb 1
      python notebooks/check_eod_data.py --ticker A --ticker B --no-splits
      python notebooks/check_eod_data.py --test --no-div --no-splits
      python notebooks/check_eod_data.py --cache --no-eod --no-splits
      python notebooks/check_eod_data.py --cleanup
    """
    if cleanup:
        main(cleanup=True)
        return
    if listed and delisted:
        raise click.UsageError("--listed and --delisted cannot be used together.")
    if no_eod and no_div and no_splits:
        raise click.UsageError("At least one data check must be enabled.")
    tickers = []
    for symbol in ticker:
        symbol = symbol.strip().upper()
        if not symbol or any(char.isspace() for char in symbol) or symbol.endswith(".JSE"):
            raise click.BadParameter("Use a non-empty ticker without the .JSE suffix.", param_hint="--ticker")
        tickers.append(f"{symbol}.JSE")
    main(verb=verb, no_eod=no_eod, no_div=no_div, no_splits=no_splits, test=test,
         no_volume=no_volume, no_zeros=no_zeros, tickers=tickers if ticker else None,
         no_clean=no_clean, use_cache=use_cache, listed=listed, delisted=delisted)


if __name__ == "__main__":
    cli()