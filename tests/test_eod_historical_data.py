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
from unittest.mock import AsyncMock, patch
import aiounittest

import datetime
import numpy as np
import pandas as pd

# Classes to be tested
from asset_base.eod_historical_data import APISessionManager, Exchanges
from asset_base.eod_historical_data import Historical
from asset_base.eod_historical_data import Bulk
from asset_base.eod_historical_data import MultiHistorical
from asset_base.eod_historical_data import date_index_name, eod_columns, dividend_columns, split_columns


def _make_eod_table(date_str="2020-01-02"):
    return pd.DataFrame(
        [
            {
                "date": date_str,
                "open": 100.0,
                "close": 101.0,
                "high": 102.0,
                "low": 99.0,
                "adjusted_close": 100.5,
                "volume": 1000000,
            }
        ]
    )


def _make_dividend_table(date_str="2020-01-02"):
    return pd.DataFrame(
        [
            {
                "date": date_str,
                "declarationDate": "2020-01-01",
                "recordDate": "2020-01-02",
                "paymentDate": "2020-01-03",
                "period": "2020-01",
                "value": 0.5,
                "unadjustedValue": 0.5,
                "currency": "USD",
            }
        ]
    )


def _make_split_table(date_str="2020-01-02"):
    return pd.DataFrame(
        [
            {
                "date": date_str,
                "split": "2/1",
            }
        ]
    )


def _make_bulk_eod_table(date_str="2020-12-31", exchange="US", tickers=None):
    if tickers is None:
        tickers = ["AAPL", "MCD"]
    rows = []
    for ticker in tickers:
        rows.append(
            {
                "date": date_str,
                "code": ticker,
                "exchange_short_name": exchange,
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "adjusted_close": 100.5,
                "volume": 1000000,
                "prev_close": 99.5,
                "change": 1.5,
                "change_p": 1.5,
            }
        )
    return pd.DataFrame(rows)


def _make_bulk_dividend_table(date_str="2020-02-07", exchange="US"):
    return pd.DataFrame(
        [
            {
                "date": date_str,
                "code": "AAPL",
                "exchange_short_name": exchange,
                "dividend": 0.2,
                "currency": "USD",
                "declarationDate": "2020-02-01",
                "recordDate": "2020-02-02",
                "paymentDate": "2020-02-03",
                "period": "2020-02",
                "unadjustedValue": 0.2,
            }
        ]
    )


def _make_bulk_split_table(date_str="2021-09-15", exchange="US"):
    return pd.DataFrame(
        [
            {
                "date": date_str,
                "code": "AAPL",
                "exchange_short_name": exchange,
                "split": "2/1",
            }
        ]
    )


def _make_exchanges_table():
    return pd.DataFrame(
        [
            {
                "Name": "USA Stocks",
                "Code": "US",
                "OperatingMIC": "XNAS, XNYS, OTCM",
                "Country": "USA",
                "Currency": "USD",
                "CountryISO2": "US",
                "CountryISO3": "USA",
            }
        ]
    )


def _make_exchange_symbols_table(exchange):
    if exchange == "INDX":
        return pd.DataFrame(
            [
                {
                    "Code": "SP500-15",
                    "Name": "S&P 500 Materials (Sector)",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
                {
                    "Code": "SP500-151010",
                    "Name": "S&P 500 Chemicals",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
                {
                    "Code": "SP500-20",
                    "Name": "S&P 500 Industrials (Sector)",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
                {
                    "Code": "SP500-25",
                    "Name": "S&P 500 Consumer Discretionary (Sector)",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
                {
                    "Code": "SP500-30",
                    "Name": "S&P 500 Consumer Staples (Sector)",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
                {
                    "Code": "SP500-35",
                    "Name": "S&P 500 Health Care (Sector)",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
                {
                    "Code": "SP500-40",
                    "Name": "S&P 500 Financials (Sector)",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
                {
                    "Code": "SP500-45",
                    "Name": "S&P 500 Information Technology (Sector)",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
                {
                    "Code": "SP500-50",
                    "Name": "S&P 500 Telecommunication Services (Sector)",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
                {
                    "Code": "SP500-55",
                    "Name": "S&P 500 Utilities (Sector)",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
                {
                    "Code": "SP500-60",
                    "Name": "S&P 500 Real Estate (Sector)",
                    "Country": "USA",
                    "Exchange": "INDX",
                    "Currency": "USD",
                    "Type": "INDEX",
                    "Isin": "",
                },
            ]
        )

    return pd.DataFrame(
        [
            {
                "Code": "WHL",
                "Name": "Woolworths Holdings Ltd",
                "Country": "South Africa",
                "Exchange": exchange,
                "Currency": "ZAC",
                "Type": "Common Stock",
                "Isin": "ZAE000063863",
            }
        ]
    )


async def _mock_api_get(endpoint, params):
    if "BAD" in endpoint:
        raise Exception("Ticker not found")

    if "/api/eod-bulk-last-day/" in endpoint:
        exchange = endpoint.rsplit("/", 1)[-1]
        bulk_type = params.get("type")
        symbols = params.get("symbols")
        if symbols:
            tickers = [item.split(".")[0] for item in symbols.split(",")]
        else:
            tickers = None
        if bulk_type == "dividends":
            return _make_bulk_dividend_table(exchange=exchange)
        if bulk_type == "splits":
            return _make_bulk_split_table(exchange=exchange)
        return _make_bulk_eod_table(exchange=exchange, tickers=tickers)

    if "/api/div/" in endpoint:
        return _make_dividend_table()

    if "/api/splits/" in endpoint:
        return _make_split_table()

    if "/api/eod/" in endpoint:
        return _make_eod_table()

    if "/api/exchanges-list" in endpoint:
        return _make_exchanges_table()

    if "/api/exchange-symbol-list/" in endpoint:
        exchange = endpoint.rsplit("/", 1)[-1]
        return _make_exchange_symbols_table(exchange)

    return pd.DataFrame()


class MockAPIMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._api_get_patcher = patch(
            "asset_base.eod_historical_data.APISessionManager.get",
            new=AsyncMock(name="APISessionManager.get", side_effect=_mock_api_get),
        )
        cls._api_get_patcher.start()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "_api_get_patcher"):
            cls._api_get_patcher.stop()
        super().tearDownClass()


def assert_date_index(tester, df):
    """Test datetime index."""
    tester.assertTrue(pd.api.types.is_datetime64_any_dtype(df.index.dtype))
    tester.assertEqual(date_index_name, df.index.name)
    tester.assertTrue(df.index.is_unique)

def assert_date_ticker_index(tester, df):
    """Test datetime, ticker index."""
    index_columns = [date_index_name, "ticker"]
    tester.assertEqual(index_columns, list(df.index.names))
    tester.assertTrue(
        pd.api.types.is_datetime64_any_dtype(
            df.index.get_level_values(date_index_name).dtype
        )
    )
    tester.assertTrue(
        pd.api.types.is_string_dtype(
            df.index.get_level_values("ticker").dtype
        )
    )
    tester.assertTrue(df.index.is_unique)

def assert_date_ticker_exchange_index(tester, df):
    """Test datetime, ticker, exchange index."""
    index_columns = [date_index_name, "ticker", "exchange"]
    tester.assertEqual(index_columns, list(df.index.names))
    tester.assertTrue(
        pd.api.types.is_datetime64_any_dtype(
            df.index.get_level_values(date_index_name).dtype
        )
    )
    tester.assertTrue(
        pd.api.types.is_string_dtype(
            df.index.get_level_values("ticker").dtype
        )
    )
    tester.assertTrue(
        pd.api.types.is_string_dtype(
            df.index.get_level_values("exchange").dtype
        )
    )
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
    tester.assertEqual(dividend_columns, list(df.columns))
    numeric_columns = {"value", "unadjustedValue"}
    for column in dividend_columns:
        if column in numeric_columns:
            tester.assertTrue(pd.api.types.is_numeric_dtype(df[column].dtype))
        else:
            tester.assertTrue(pd.api.types.is_string_dtype(df[column].dtype))

def assert_split_columns(tester, df):
    """Test split DataFame columns."""
    tester.assertIsInstance(df, pd.DataFrame)
    tester.assertEqual(split_columns, list(df.columns))
    tester.assertTrue(pd.api.types.is_string_dtype(df["split"].dtype))


class TestAPISessionManager(aiounittest.AsyncTestCase):
    """Direct API query, response and result checking."""

    @classmethod
    def setUpClass(cls):
        """Set up class test fixtures."""
        super().setUpClass()
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
        super().tearDownClass()

    def setUp(self):
        """Set up one test."""
        super().setUp()

    def tearDown(self):
        """tear down test case fixtures."""
        super().tearDown()

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


class TestHistorical(MockAPIMixin, aiounittest.AsyncTestCase):
    """Using security AAPL.US (Apple Inc.)."""

    @classmethod
    def setUpClass(cls):
        """Set up class test fixtures."""
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        """Tear down class test fixtures."""
        super().tearDownClass()

    def setUp(self):
        """Set up one test."""
        super().setUp()

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
            df = await historical.get_dividends("JSE", "STX40", from_date, to_date)
        assert_date_index(self, df)
        assert_dividend_columns(self, df)

    async def test_get_splits(self):
        """Get daily, split historical over a date range."""
        # Test data
        from_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        to_date = datetime.datetime.strptime("2021-12-31", "%Y-%m-%d")
        # Get
        async with Historical() as historical:
            df = await historical.get_splits("US", "AAPL", from_date, to_date)
        assert_date_index(self, df)
        assert_split_columns(self, df)

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


class TestBulk(MockAPIMixin, aiounittest.AsyncTestCase):
    """Using security AAPL.US (Apple Inc.) and MCD.US (McDonald's Inc.)."""

    @classmethod
    def setUpClass(cls):
        """Set up class test fixtures."""
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        """Tear down class test fixtures."""
        super().tearDownClass()

    def setUp(self):
        """Set up one test."""
        super().setUp()

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
        self.assertEqual(columns, list(df.columns))
        self.assertTrue(pd.api.types.is_numeric_dtype(df["dividend"].dtype))
        self.assertTrue(pd.api.types.is_numeric_dtype(df["unadjustedValue"].dtype))
        for column in ["currency", "declarationDate", "recordDate", "paymentDate", "period"]:
            self.assertTrue(pd.api.types.is_string_dtype(df[column].dtype))
        self.assertEqual(index_names, df.index.names)
        self.assertTrue(
            pd.api.types.is_datetime64_any_dtype(
                df.index.get_level_values("date").dtype
            )
        )
        self.assertTrue(
            pd.api.types.is_string_dtype(
                df.index.get_level_values("ticker").dtype
            )
        )
        self.assertTrue(
            pd.api.types.is_string_dtype(
                df.index.get_level_values("exchange").dtype
            )
        )
        # The index is too long to test content.

    async def test_get_splits(self):
        """Get bulk EOD splits for the exchange on a date."""
        date = datetime.datetime.strptime("2021-09-15", "%Y-%m-%d")
        async with Bulk() as bulk:
            df = await bulk.get_splits("US", date=date)
        # Test DataFame structure
        index_names = ["date", "ticker", "exchange"]
        columns = ["split"]
        self.assertEqual(columns, list(df.columns))
        self.assertTrue(pd.api.types.is_string_dtype(df["split"].dtype))
        self.assertEqual(index_names, df.index.names)
        self.assertTrue(
            pd.api.types.is_datetime64_any_dtype(
                df.index.get_level_values("date").dtype
            )
        )
        self.assertTrue(
            pd.api.types.is_string_dtype(
                df.index.get_level_values("ticker").dtype
            )
        )
        self.assertTrue(
            pd.api.types.is_string_dtype(
                df.index.get_level_values("exchange").dtype
            )
        )
        # The index is too long to test content.


class TestExchanges(MockAPIMixin, unittest.TestCase):
    """Get exchanges (and list of indices) data."""

    @classmethod
    def setUpClass(cls):
        """Set up class test fixtures."""
        super().setUpClass()
        cls.exchanges = Exchanges()
        cls.exchange = "JSE"

    def setUp(self):
        """Set up one test."""
        super().setUp()

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
        # NOTE: Here XNAS, XNYS, OTCM are the OperatingMICs for USA Stocks and
        # are a comma-separated string.
        test_row = ['USA Stocks', 'US', 'XNAS, XNYS, OTCM', 'USA', 'USD', 'US', 'USA']
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


class TestMultiHistorical(MockAPIMixin, unittest.TestCase):
    """Get bulk histories across exchanges, securities and date ranges."""

    @classmethod
    def setUpClass(cls):
        """Set up class test fixtures."""
        super().setUpClass()
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
        super().tearDownClass()

    def setUp(self):
        """Set up one test."""
        super().setUp()

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

    def test_get_splits(self):
        """Get historical data for a list of securities."""
        # Test data
        to_date = datetime.datetime.strptime("2021-12-31", "%Y-%m-%d")
        # Longer date range test causes a decision to use the EOD API service
        from_date1 = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        symbol_list = [(s[0], s[1], from_date1, to_date) for s in self.symbol_list]
        df = self.historical.get_splits(symbol_list)
        assert_date_ticker_exchange_index(self, df)
        assert_split_columns(self, df)

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
            TestAPISessionManager,
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
