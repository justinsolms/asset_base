#!/usr/bin/env unittest
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Test suite for the ``eod_historical_data`` module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the asset_base module.
The asset_base module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
import asyncio
from io import StringIO
import unittest
import aiounittest

import datetime
import numpy as np
import pandas as pd

# Classes to be tested
from src.asset_base.eod_historical_data import APISessionManager, Exchanges
from src.asset_base.eod_historical_data import Historical
from src.asset_base.eod_historical_data import Bulk
from src.asset_base.eod_historical_data import MultiHistorical
from src.asset_base.eod_historical_data import date_index_name, eod_columns, dividend_columns


def assert_date_index(tester, df):
    """Test datetime index."""
    index_type = np.dtype("datetime64[ns]")
    tester.assertEqual(index_type, df.index.dtype)
    tester.assertEqual(date_index_name, df.index.name)
    tester.assertTrue(df.index.is_unique)


def assert_date_ticker_index(tester, df):
    """Test datetime, ticker index."""
    index_columns = [date_index_name, "ticker"]
    index_types = [np.dtype("datetime64[ns]"), np.dtype("object")]
    test_df = pd.Series(index_types, index=index_columns)  #
    pd.testing.assert_series_equal(test_df, df.index.dtypes)
    tester.assertTrue(df.index.is_unique)


def assert_date_ticker_exchange_index(tester, df):
    """Test datetime, ticker, exchange index."""
    index_columns = [date_index_name, "ticker", "exchange"]
    index_types = [np.dtype("datetime64[ns]"), np.dtype("object"), np.dtype("object")]
    test_df = pd.Series(index_types, index=index_columns)  #
    pd.testing.assert_series_equal(test_df, df.index.dtypes)
    tester.assertTrue(df.index.is_unique)


def assert_eod_columns(tester, df):
    """Test EOD DataFame columns."""
    tester.assertIsInstance(df, pd.DataFrame)
    # Test columns
    column_types = [
        np.dtype("float64"),
        np.dtype("float64"),
        np.dtype("float64"),
        np.dtype("float64"),
        np.dtype("float64"),
        np.dtype("int64"),
    ]
    test_df = pd.Series(column_types, index=eod_columns)  #
    pd.testing.assert_series_equal(test_df, df.dtypes)


def assert_dividend_columns(tester, df):
    """Test dividend DataFame columns."""
    tester.assertIsInstance(df, pd.DataFrame)
    # Test columns
    column_types = [
        np.dtype("object"),
        np.dtype("object"),
        np.dtype("object"),
        np.dtype("object"),
        np.dtype("float64"),
        np.dtype("float64"),
        np.dtype("object"),
    ]
    test_df = pd.Series(column_types, index=dividend_columns)  #
    pd.testing.assert_series_equal(test_df, df.dtypes)


class TestAPI(aiounittest.AsyncTestCase):
    """Direct API query, response and result checking."""

    @classmethod
    def setUpClass(cls):
        """Set up class test fixtures."""
        domain = "eodhistoricaldata.com"
        service = "/api/eod"
        ticker1 = "STX40"
        ticker2 = "STXIND"
        ticker_bad = "BADTICKER"
        exchange = "JSE"

        # Path must append ticker and short exchange code to service
        endpoint1 = "{}/{}.{}".format(service, ticker1, exchange)
        endpoint2 = "{}/{}.{}".format(service, ticker2, exchange)
        endpoint_bad = "{}/{}.{}".format(service, ticker_bad, exchange)

        cls.url1 = f"https://{domain}{endpoint1}"
        cls.url2 = f"https://{domain}{endpoint2}"
        cls.url_bad = f"https://{domain}{endpoint_bad}"

        from_date = "2022-01-01"
        to_date = "2022-01-07"
        cls.params = dict(
            from_date=from_date,
            to_date=to_date,
            fmt="json",  # Default to CSV table. See NOTE in _get_retries!
            period="d",  # Default to daily sampling period
            order="a",  # Default to ascending order
        )
        cls.endpoint1 = endpoint1
        cls.endpoint2 = endpoint2
        cls.endpoint_bad = endpoint_bad

    @classmethod
    def tearDownClass(cls):
        """Tear down class test fixtures."""
        pass

    def setUp(self):
        """Set up one test."""
        pass

    def tearDown(self):
        """tear down test case fixtures."""
        pass

    def assert_df(self, df):
        # Set up for testing
        df["date"] = pd.to_datetime(df["date"])
        df.set_index(date_index_name, inplace=True)
        df = df[eod_columns]
        df["open"] = df["open"].astype("float64")
        # Check
        assert_date_index(self, df)
        assert_eod_columns(self, df)

    async def test___init__(self):
        """Test Initialization."""
        async with APISessionManager() as api:
            self.assertIsInstance(api, APISessionManager)

    async def test_get(self):
        """Get with the possibility of retries to the API."""
        async with APISessionManager() as api:
            df = await api.get(self.endpoint1, self.params)
        self.assert_df(df)

    async def test_bad_ticker(self):
        """Fail with ticker not found."""
        with self.assertRaises(Exception):
            async with APISessionManager() as api:
                await api.get(self.endpoint_bad, self.params)

    def test_runner(self):
        """Get multiple requests tasks in the runner."""

        # Data Gathering awaitable
        async def get_results():
            tasks_list = list()
            async with APISessionManager() as api:
                tasks_list.append(api.get(self.endpoint1, self.params))
                tasks_list.append(api.get(self.endpoint2, self.params))
                results = await asyncio.gather(*tasks_list, return_exceptions=True)
            return results

        # Run all tasks
        df1, df2 = asyncio.run(get_results())
        # Check
        self.assert_df(df1)
        self.assert_df(df2)


class TestHistorical(aiounittest.AsyncTestCase):
    """Using security AAPL.US (Apple Inc.)."""

    @classmethod
    def setUpClass(cls):
        """Set up class test fixtures."""
        pass

    @classmethod
    def tearDownClass(cls):
        """Tear down class test fixtures."""
        pass

    def setUp(self):
        """Set up one test."""
        pass

    async def test___init__(self):
        """Test Initialization."""
        # Is this a subclass of _API?
        async with Historical() as historical:
            self.assertIsInstance(historical, APISessionManager)

    async def test_get_eod(self):
        """Get daily, EOD historical over a date range."""
        # Test data
        from_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        to_date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d")
        # Get
        async with Historical() as historical:
            df = await historical.get_eod("US", "AAPL", from_date, to_date)
        # Test DataFame structure
        assert_date_index(self, df)
        assert_eod_columns(self, df)

    async def test_get_dividends(self):
        """Get daily, dividend historical over a date range."""
        # Test data
        from_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        to_date = datetime.datetime.strptime("2020-11-06", "%Y-%m-%d")
        # Get
        async with Historical() as historical:
            df = await historical.get_dividends("US", "AAPL", from_date, to_date)
        assert_date_index(self, df)
        assert_dividend_columns(self, df)

    async def test_get_forex(self):
        """Get daily, EOD historial forex (USD based) over a date range."""
        # Test data
        from_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        to_date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d")
        # Get
        async with Historical() as historical:
            df = await historical.get_forex("EURGBP", from_date, to_date)
        # Test DataFame structure
        assert_date_index(self, df)
        assert_eod_columns(self, df)


class TestBulk(aiounittest.AsyncTestCase):
    """Using security AAPL.US (Apple Inc.) and MCD.US (McDonald's Inc.)."""

    @classmethod
    def setUpClass(cls):
        """Set up class test fixtures."""
        pass

    @classmethod
    def tearDownClass(cls):
        """Tear down class test fixtures."""
        pass

    def setUp(self):
        """Set up one test."""
        pass

    async def test___init__(self):
        """Test Initialization."""
        # Is this a subclass of _API?
        async with Bulk() as bulk:
            self.assertIsInstance(bulk, APISessionManager)

    async def test_get_eod(self):
        """Get bulk EOD price and volume for the exchange on a date."""
        date = datetime.datetime.strptime("2021-01-03", "%Y-%m-%d")
        async with Bulk() as bulk:
            df = await bulk.get_eod("US", date=date, symbols=["AAPL", "MCD"])
        # Test DataFame structure
        index_names = ["date", "ticker", "exchange"]
        columns = [
            "open",
            "high",
            "low",
            "close",
            "adjusted_close",
            "volume",
            "prev_close",
            "change",
            "change_p",
        ]
        index_type = np.dtype("object")
        column_types = [
            np.dtype("float64"),
            np.dtype("float64"),
            np.dtype("float64"),
            np.dtype("float64"),
            np.dtype("float64"),
            np.dtype("int64"),
            np.dtype("float64"),
            np.dtype("float64"),
            np.dtype("float64"),
        ]
        test_df = pd.Series(column_types, index=columns)
        pd.testing.assert_series_equal(test_df, df.dtypes)
        self.assertEqual(index_type, df.index.dtype)
        self.assertEqual(index_names, df.index.names)
        test_index_list = [
            (pd.Timestamp("2020-12-31 00:00:00"), "AAPL", "US"),
            (pd.Timestamp("2020-12-31 00:00:00"), "MCD", "US"),
        ]
        self.assertEqual(test_index_list, df.index.tolist())

    async def test_get_dividends(self):
        """Get bulk EOD dividends for the exchange on a date."""
        date = datetime.datetime.strptime("2020-02-07", "%Y-%m-%d")
        async with Bulk() as bulk:
            df = await bulk.get_dividends("US", date=date)
        # Test DataFame structure
        index_names = ["date", "ticker", "exchange"]
        columns = [
            "dividend",
            "currency",
            "declarationDate",
            "recordDate",
            "paymentDate",
            "period",
            "unadjustedValue",
        ]
        index_type = np.dtype("object")
        column_types = [
            np.dtype("float64"),
            np.dtype("object"),
            np.dtype("object"),
            np.dtype("object"),
            np.dtype("object"),
            np.dtype("object"),
            np.dtype("float64"),
        ]
        test_df = pd.Series(column_types, index=columns)
        pd.testing.assert_series_equal(test_df, df.dtypes)
        self.assertEqual(index_type, df.index.dtype)
        self.assertEqual(index_names, df.index.names)
        # The index is too long to test content.

    async def test_get_splits(self):
        """Get bulk EOD splits for the exchange on a date."""
        date = datetime.datetime.strptime("2021-09-15", "%Y-%m-%d")
        async with Bulk() as bulk:
            df = await bulk.get_splits("US", date=date)
        # Test DataFame structure
        index_names = ["date", "ticker", "exchange"]
        columns = ["split"]
        index_type = np.dtype("object")
        column_types = [np.dtype("object")]
        test_df = pd.Series(column_types, index=columns)
        pd.testing.assert_series_equal(test_df, df.dtypes)
        self.assertEqual(index_type, df.index.dtype)
        self.assertEqual(index_names, df.index.names)
        # The index is too long to test content.


class TestExchanges(unittest.TestCase):
    """Get exchanges (and list of indices) data."""

    @classmethod
    def setUpClass(cls):
        """Set up class test fixtures."""
        cls.exchanges = Exchanges()
        cls.exchange = "JSE"

    def setUp(self):
        """Set up one test."""
        pass

    def test_get_exchanges(self):
        """Get the full list of supported exchanges."""
        test_columns = [
            "Name",
            "Code",
            "OperatingMIC",
            "Country",
            "Currency",
            "CountryISO2",
            "CountryISO3",
        ]
        test_row = ["USA Stocks", "US", "XNAS, XNYS", "USA", "USD", "US", "USA"]
        table = self.exchanges.get_exchanges()
        self.assertEqual(test_columns, table.columns.tolist())
        self.assertEqual(
            test_row, table[table["Name"] == "USA Stocks"].values.tolist()[0]
        )

    def test_get_exchange_symbol_list(self):
        """Get the full list symbols (tickers) on the exchange."""
        test_columns = [
            "Code",
            "Name",
            "Country",
            "Exchange",
            "Currency",
            "Type",
            "Isin",
        ]
        test_row = [
            "WHL",
            "Woolworths Holdings Ltd",
            "South Africa",
            "JSE",
            "ZAC",
            "Common Stock",
            "ZAE000063863",
        ]
        table = self.exchanges.get_exchange_symbols(self.exchange)
        self.assertEqual(test_columns, table.columns.tolist())
        self.assertEqual(test_row, table[table["Code"] == "WHL"].values.tolist()[0])

    def test_get_indices_list(self):
        """Get a list of supported indices."""
        # Test data
        test_csv = (
            "Code,Name,Country,Exchange,Currency,Type\n"
            "SP500-15,S&P 500 Materials (Sector),USA,INDX,USD,INDEX\n"
            "SP500-151010,S&P 500 Chemicals,USA,INDX,USD,INDEX\n"
            "SP500-20,S&P 500 Industrials (Sector),USA,INDX,USD,INDEX\n"
            "SP500-25,S&P 500 Consumer Discretionary (Sector),USA,INDX,USD,INDEX\n"
            "SP500-30,S&P 500 Consumer Staples (Sector),USA,INDX,USD,INDEX\n"
            "SP500-35,S&P 500 Health Care (Sector),USA,INDX,USD,INDEX\n"
            "SP500-40,S&P 500 Financials (Sector),USA,INDX,USD,INDEX\n"
            "SP500-45,S&P 500 Information Technology (Sector),USA,INDX,USD,INDEX\n"
            "SP500-50,S&P 500 Telecommunication Services (Sector),USA,INDX,USD,INDEX\n"
            "SP500-55,S&P 500 Utilities (Sector),USA,INDX,USD,INDEX\n"
            "SP500-60,S&P 500 Real Estate (Sector),USA,INDX,USD,INDEX\n"
        )
        test_io = StringIO(test_csv)   # Convert String into StringIO
        test_df = pd.read_csv(test_io)
        # Call
        df = self.exchanges.get_indices()
        df = df[df.Code.isin(test_df.Code)]
        df.drop(columns='Isin', inplace=True)  # Empty in data
        # Sort rows by ticker and columns by name
        test_df = test_df.sort_values('Code').sort_index(axis='columns').reset_index(drop=True)
        df = df.sort_values('Code').sort_index(axis='columns').reset_index(drop=True)
        # Test
        pd.testing.assert_frame_equal(test_df, df)


class TestMultiHistorical(unittest.TestCase):
    """Get bulk histories across exchanges, securities and date ranges."""

    @classmethod
    def setUpClass(cls):
        """Set up class test fixtures."""
        cls.historical = MultiHistorical()
        cls.symbol_list = (("AAPL", "US"), ("MCD", "US"), ("STX40", "JSE"))
        cls.forex_list = ("USDEUR", "USDGBP", "USDUSD")
        cls.index_list = ("GSPC", "ASX", "J200")
        cls.symbol_list_bad = (
            ("AAPL", "US"),
            ("MCD", "US"),
            ("BADONE", "JSE"),
            ("STX40", "JSE"),
        )

    @classmethod
    def tearDownClass(cls):
        """Tear down class test fixtures."""
        pass

    def setUp(self):
        """Set up one test."""
        pass

    def test___init__(self):
        """Test Initialization."""
        self.assertIsInstance(self.historical, MultiHistorical)

    def test__get_eod(self):
        """Get historical data for a list of securities."""
        # Test data
        from_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        to_date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d")
        # Use EOD API
        symbol_list = [(s[0], s[1], from_date, to_date) for s in self.symbol_list]
        df = asyncio.run(
            self.historical._get_eod(Historical._historical_eod, symbol_list)
        )
        # Set up df for testing
        df = df[eod_columns]
        # Check
        assert_date_ticker_exchange_index(self, df)
        assert_eod_columns(self, df)

    @unittest.skip("Work in progress")
    def test__get_bulk(self):
        """Get bulk historical data for a range of dates."""
        # Test data
        from_date = datetime.datetime.strptime("2020-12-24", "%Y-%m-%d")
        to_date = datetime.datetime.strptime("2020-12-30", "%Y-%m-%d")
        columns = [
            "change",
            "change_p",
            "close",
            "high",
            "low",
            "open",
            "prev_close",
            "volume",
        ]
        index_names = ["date", "ticker", "exchange"]
        # NOTE: This data may change as EOD historical make corrections
        test_values = [
            [-1.15, -0.8527, 133.72, 135.99, 133.4, 135.58, 134.87, 96452100.0],
            [-1.15, -0.5406, 211.56, 213.36, 211.28, 212.96, 212.71, 1855000.0],
            [38.0, 0.6983, 5480.0, 5510.0, 5385.0, 5405.0, 5442.0, 57423.0],
        ]
        # Get Bulk EOD (Type=None)
        df = asyncio.run(
            self.historical._get_bulk(self.symbol_list, from_date, to_date, type=None)
        )
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns="adjusted_close", inplace=True)
        # Test-rank columns
        df = df[columns]
        # Test
        self.assertEqual(set(df.index.names), set(index_names))
        self.assertEqual(set(df.columns), set(columns))
        df = df.loc[to_date]
        self.assertFalse(df.empty)
        for i, item in enumerate(df.iterrows()):
            symbol, series = item
            self.assertEqual(
                test_values[i],
                series.tolist(),
            )

    def test_get_eod(self):
        """Get historical data for a list of securities."""
        # Test data
        to_date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d")
        # Longer date range test causes a decision to use the EOD API service
        from_date1 = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        symbol_list_bad = [
            (s[0], s[1], from_date1, to_date) for s in self.symbol_list_bad
        ]
        df = self.historical.get_eod(symbol_list_bad)
        assert_date_ticker_exchange_index(self, df)
        assert_eod_columns(self, df)

        # Shorter date range test causes a decision to use the Bulk API service
        from_date2 = datetime.datetime.strptime("2020-12-25", "%Y-%m-%d")
        symbol_list = [(s[0], s[1], from_date2, to_date) for s in self.symbol_list]
        df = self.historical.get_eod(symbol_list)
        # Check
        assert_date_ticker_exchange_index(self, df)
        assert_eod_columns(self, df)

    def test_get_dividends(self):
        """Get historical data for a list of securities."""
        # Test data
        to_date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d")
        # Longer date range test causes a decision to use the EOD API service
        from_date1 = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        symbol_list = [(s[0], s[1], from_date1, to_date) for s in self.symbol_list]
        df = self.historical.get_dividends(symbol_list)
        assert_date_ticker_exchange_index(self, df)
        assert_dividend_columns(self, df)

    def test_get_forex(self):
        """Get daily, EOD historial forex."""
        # Test data
        from_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        to_date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d")
        # Get
        forex_list = [(s, from_date, to_date) for s in self.forex_list]
        df = self.historical.get_forex(forex_list)
        assert_date_ticker_index(self, df)
        assert_eod_columns(self, df)

    def test_get_index(self):
        """Get daily, EOD historial forex."""
        # Test data
        from_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        to_date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d")
        # Get
        index_list = [(s, from_date, to_date) for s in self.index_list]
        df = self.historical.get_index(index_list)
        assert_date_ticker_index(self, df)
        assert_eod_columns(self, df)


class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        test_classes = [
            TestAPI,
            TestHistorical,
            TestBulk,
            TestMultiHistorical,
        ]

        suites_list = list()
        loader = unittest.TestLoader()
        for test_class in test_classes:
            suites_list.append(loader.loadTestsFromTestCase(test_class))

        suite.addTests(suites_list)

        self.suite = suite

    def run(self):
        runner = unittest.TextTestRunner()
        runner.run(self.suite)


if __name__ == "__main__":
    suite = Suite()
    suite.run()
