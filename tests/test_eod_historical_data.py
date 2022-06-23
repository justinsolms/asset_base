#!/usr/bin/env unittest
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Test suite for the financial_feed module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the fundmanage module.
The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
import asyncio
import unittest
from aiohttp import ClientConnectorError, ContentTypeError
import aiounittest

import pandas as pd
import datetime

# Classes to be tested
from asset_base.eod_historical_data import _API
from asset_base.eod_historical_data import Historical
from asset_base.eod_historical_data import Bulk
from asset_base.eod_historical_data import BulkHistorical
from fundmanage.utils import date_to_str

import warnings
warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class TestAPI(aiounittest.AsyncTestCase):
    """ Direct API query, response and result checking. """

    @classmethod
    def setUpClass(cls):
        """ Set up class test fixtures. """
        domain = 'eodhistoricaldata.com'
        service = '/api/eod'
        ticker1 = 'AAPL'
        ticker2 = 'MCD'
        ticker_bad = 'BADTICKER'
        exchange = 'US'

        # Path must append ticker and short exchange code to service
        path1 = '{}/{}.{}'.format(service, ticker1, exchange)
        path2 = '{}/{}.{}'.format(service, ticker2, exchange)
        path_bad = '{}/{}.{}'.format(service, ticker_bad, exchange)

        cls.url1 = f'https://{domain}{path1}'
        cls.url2 = f'https://{domain}{path2}'
        cls.url_bad = f'https://{domain}{path_bad}'

        from_date = '2022-01-01'
        to_date = '2022-01-07'
        cls.params = dict(
            from_date=from_date,
            to_date=to_date,
            fmt='json',  # Default to CSV table. See NOTE in _get_retries!
            period='d',  # Default to daily sampling period
            order='a',  # Default to ascending order
        )
        cls.path1 = path1
        cls.path2 = path2
        cls.path_bad = path_bad

    @classmethod
    def tearDownClass(cls):
        """ Tear down class test fixtures. """
        pass

    def setUp(self):
        """ Set up one test. """
        pass

    def tearDown(self):
        """tear down test case fixtures."""
        pass

    async def test___init__(self):
        """ Test Initialization. """
        async with _API() as api:
            self.assertIsInstance(api, _API)

    async def test__get_retries(self):
        """Get with the possibility of retries to the API."""
        index_names = ['date', 'open', 'high', 'low', 'close']
        async with _API() as api:
            response = await api._get_retries(self.path1, self.params)
            # Check
            self.assertIsInstance(response, pd.DataFrame)
            self.assertEqual(index_names, response.columns.to_list()[0:5])

    async def test_bad_ticker(self):
        """Fail with ticker not found."""
        with self.assertRaises(Exception) as ex:
            async with _API() as api:
                await api._get_retries(self.path_bad, self.params)

    def test_runner(self):
        """Get multiple requests tasks in the runner."""
        # Data Gathering awaitable
        index_names = ['date', 'open', 'high', 'low', 'close']

        async def get_results():
            tasks_list = list()
            async with _API() as api:
                tasks_list.append(api._get_retries(self.path1, self.params))
                tasks_list.append(api._get_retries(self.path2, self.params))
                results = await asyncio.gather(*tasks_list, return_exceptions=True)
            return results

        # Run all tasks
        response1, response2 = asyncio.run(get_results())
        # Check
        self.assertIsInstance(response1, pd.DataFrame)
        self.assertEqual(index_names, response1.columns.to_list()[0:5])
        self.assertIsInstance(response2, pd.DataFrame)
        self.assertEqual(index_names, response2.columns.to_list()[0:5])


class TestHistorical(aiounittest.AsyncTestCase):
    """ Using security AAPL.US (Apple Inc.). """

    @classmethod
    def setUpClass(cls):
        """ Set up class test fixtures. """
        pass

    @classmethod
    def tearDownClass(cls):
        """ Tear down class test fixtures. """
        pass

    def setUp(self):
        """ Set up one test. """
        pass

    async def test___init__(self):
        """ Test Initialization. """
        # Is this a subclass of _API?
        async with Historical() as historical:
            self.assertIsInstance(historical, _API)

    async def test_get_eod(self):
        """ Get daily, EOD historical over a date range. """
        # Test data
        from_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
        to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d')
        columns = ['open', 'high', 'low', 'close', 'volume']
        index = ['date']
        # NOTE: This data may change as EOD historical make corrections
        values = [134.08, 134.74, 131.72, 132.69, 99116594.0]
        # Get
        async with Historical() as historical:
            df = await historical.get_eod('US', 'AAPL', from_date, to_date)
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns='adjusted_close', inplace=True)
        # Test DataFame structure
        self.assertEqual(index, list(df.index.names))
        self.assertEqual(columns, list(df.columns))
        # Test-rank columns
        df = df[columns]
        # Test data
        self.assertEqual(len(df), 253)
        self.assertEqual(
            values,
            df.loc[to_date].tolist(),
            )

    async def test_get_dividends(self):
        """ Get daily, dividend historical over a date range. """
        # Test data
        from_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
        to_date = datetime.datetime.strptime('2020-11-06', '%Y-%m-%d')
        columns = ['declarationDate', 'recordDate', 'paymentDate', 'period',
                   'value', 'unadjustedValue', 'currency']
        index = ['date']
        # NOTE: This data may change as EOD historical make corrections
        values = ['2020-10-29', '2020-11-09', '2020-11-12', 'Quarterly',
                  0.205, 0.205, 'USD']
        # Get
        async with Historical() as historical:
            df = await historical.get_dividends('US', 'AAPL', from_date, to_date)
        # Test DataFame structure
        self.assertEqual(index, list(df.index.names))
        self.assertEqual(columns, list(df.columns))
        # Test-rank columns
        df = df[columns]
        # Test
        self.assertEqual(len(df), 4)
        self.assertEqual(
            values,
            df.loc[to_date].tolist(),
            )

    async def test_get_forex(self):
        """ Get daily, EOD historial forex (USD based) over a date range. """
        # Test data
        from_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
        to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d')
        columns = ['close', 'high', 'low', 'open', 'volume']
        # NOTE: This data may change as EOD historical make corrections
        values = [0.8944, 0.9025, 0.8943, 0.9024, 166330.0]
        # Get
        async with Historical() as historical:
            df = await historical.get_forex('EURGBP', from_date, to_date)
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns='adjusted_close', inplace=True)
        # Test-rank columns
        df = df[columns]
        # Test
        self.assertEqual(314, len(df))
        self.assertEqual(
            values,
            df.loc[to_date].tolist(),
            )


class TestBulk(aiounittest.AsyncTestCase):
    """ Using security AAPL.US (Apple Inc.) and MCD.US (McDonald's Inc.). """

    @classmethod
    def setUpClass(cls):
        """ Set up class test fixtures. """
        pass

    @classmethod
    def tearDownClass(cls):
        """ Tear down class test fixtures. """
        pass

    def setUp(self):
        """ Set up one test. """
        pass

    async def test___init__(self):
        """ Test Initialization. """
        # Is this a subclass of _API?
        async with Bulk() as bulk:
            self.assertIsInstance(bulk, _API)

    async def test_get_eod(self):
        """ Get bulk EOD price and volume for the exchange on a date. """
        columns = ['open', 'high', 'low', 'close', 'adjusted_close', 'volume',
                   'prev_close', 'change', 'change_p']
        index = ['date', 'ticker', 'exchange']
        date = datetime.datetime.strptime('2021-01-03', '%Y-%m-%d')
        # NOTE: This data may change as EOD historical make corrections
        data = [134.08, 134.74, 131.72, 132.69, 131.516,
                99116594.0, 133.72, -1.03, -0.7703]
        async with Bulk() as bulk:
            df = await bulk.get_eod('US', date=date, symbols=['AAPL', 'MCD'])
        # Test DataFame structure
        self.assertEqual(index, list(df.index.names))
        self.assertEqual(columns, list(df.columns))
        # Test data content
        self.assertEqual(len(df), 2)
        self.assertEqual(
            df.index.tolist(),
            [
                (pd.Timestamp('2020-12-31 00:00:00'), 'AAPL', 'US'),
                (pd.Timestamp('2020-12-31 00:00:00'), 'MCD', 'US')
            ]
        )
        self.assertEqual(
            data,
            df.loc['2020-12-31', 'AAPL', 'US'].tolist(),
            )

    async def test_get_dividends(self):
        """ Get bulk EOD dividends for the exchange on a date. """
        columns = ['dividend', 'currency', 'declarationDate', 'recordDate',
                   'paymentDate', 'period', 'unadjustedValue']
        index = ['date', 'ticker', 'exchange']
        date = datetime.datetime.strptime('2020-02-07', '%Y-%m-%d')
        # NOTE: This data may change as EOD historical make corrections
        data = [0.1925, 'USD', '2020-01-28', '2020-02-10', '2020-02-13',
                'Quarterly', 0.77]
        async with Bulk() as bulk:
            df = await bulk.get_dividends('US', date=date)
        # Test DataFame structure
        self.assertEqual(index, list(df.index.names))
        self.assertEqual(columns, list(df.columns))
        # Test data
        df = df[columns]
        self.assertEqual(
            data,
            df.loc['2020-02-07', 'AAPL', 'US'].tolist(),
            )

    async def test_get_splits(self):
        """ Get bulk EOD splits for the exchange on a date. """
        columns = ['split']
        index = ['date', 'ticker', 'exchange']
        # NOTE: This data may change as EOD historical make corrections
        data = ['1.000000/20.000000']
        date = datetime.datetime.strptime('2021-09-15', '%Y-%m-%d')
        async with Bulk() as bulk:
            df = await bulk.get_splits('US', date=date)
        # Test DataFame structure
        self.assertEqual(index, list(df.index.names))
        self.assertEqual(columns, list(df.columns))
        # Test data
        self.assertEqual(
            data,
            df.loc['2021-09-15', 'SMATF', 'US'].tolist(),
        )


class TestBulkHistorical(unittest.TestCase):
    """ Get bulk histories across exchanges, securities and date ranges."""

    @classmethod
    def setUpClass(cls):
        """ Set up class test fixtures. """
        cls.bulk = BulkHistorical()
        cls.symbol_list = (('AAPL', 'US'), ('MCD', 'US'), ('STX40', 'JSE'))
        cls.forex_list = ('USDEUR', 'USDGBP', 'USDUSD')
        cls.symbol_list_bad = (
            ('AAPL', 'US'), ('MCD', 'US'), ('BADONE', 'JSE'), ('STX40', 'JSE'))

    @classmethod
    def tearDownClass(cls):
        """ Tear down class test fixtures. """
        pass

    def setUp(self):
        """ Set up one test. """
        pass

    def test___init__(self):
        """ Test Initialization. """
        self.assertIsInstance(self.bulk, BulkHistorical)

    def test__get_eod(self):
        """ Get historical data for a list of securities. """
        # Test data
        from_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
        to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d')
        index_names = ['date', 'ticker', 'exchange']
        columns = ['close', 'high', 'low', 'open', 'volume']
        # NOTE: This data may change as EOD historical make corrections
        test_values = [
            [132.69, 134.74, 131.72, 134.08, 99116594.0],
            [214.58, 214.93, 210.78, 211.25, 2610914.0],
            [5460.0, 5511.0, 5403.0, 5492.0, 112700.0]
        ]
        # Get
        # Use EOD API
        df = asyncio.run(
            self.bulk._get_eod(
                Historical._historical_eod, self.symbol_list,
                from_date, to_date,
                )
        )
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns='adjusted_close', inplace=True)
        # Test-rank columns
        df = df[columns]
        # Test
        self.assertEqual(len(df), 758)
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

    def test__get_bulk(self):
        """ Get bulk historical data for a range of dates. """
        # Test data
        from_date = datetime.datetime.strptime('2020-12-24', '%Y-%m-%d')
        to_date = datetime.datetime.strptime('2020-12-30', '%Y-%m-%d')
        columns = [
            'change', 'change_p', 'close', 'high', 'low', 'open', 'prev_close', 'volume']
        index_names = ['date', 'ticker', 'exchange']
        # NOTE: This data may change as EOD historical make corrections
        test_values = [
            [-1.15, -0.8527, 133.72, 135.99, 133.4, 135.58, 134.87, 96452117.0],
            [-1.15, -0.5406, 211.56, 213.36, 211.28, 212.96, 212.71, 1854990.0],
            [38.0, 0.6983, 5480.0, 5510.0, 5385.0, 5405.0, 5442.0, 57423.0],
        ]
        # Get Bulk EOD (Type=None)
        df = asyncio.run(
            self.bulk._get_bulk(
                self.symbol_list,
                from_date, to_date,
                type=None
                )
            )
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns='adjusted_close', inplace=True)
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
        """ Get historical data for a list of securities. """
        # Test data
        to_date = '2020-12-31'
        index_names = ['date', 'ticker', 'exchange']
        columns = ['close', 'high', 'low', 'open', 'volume']
        test_values = [  # Last date data
            [132.69, 134.74, 131.72, 134.08, 99116594.0],
            [214.58, 214.93, 210.78, 211.25, 2610914.0],
            [5460.0, 5511.0, 5403.0, 5492.0, 112700.0]
        ]

        # Longer date range test causes a decision to use the EOD API service
        from_date1 = '2020-01-01'
        df = self.bulk.get_eod(self.symbol_list_bad, from_date1, to_date)
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns='adjusted_close', inplace=True)
        # Test-rank columns
        df = df[columns]
        df1 = df.copy()
        # Test
        self.assertEqual(len(df), 758)
        self.assertEqual(list(df.index.names), list(index_names))
        self.assertEqual(list(df.columns), list(columns))
        # Test against last date data
        df = df.loc[to_date].droplevel('date')
        self.assertFalse(df.empty)
        for i, item in enumerate(df.iterrows()):
            symbol, series = item
            self.assertEqual(
                test_values[i],
                series.tolist(),
                )

        # Shorter date range test causes a decision to use the Bulk API service
        from_date2 = '2020-12-25'
        df = self.bulk.get_eod(self.symbol_list, from_date2, to_date)
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns='adjusted_close', inplace=True)
        # Test-rank columns
        df = df[columns]
        df2 = df.copy()
        # Test
        self.assertEqual(set(df.index.names), set(index_names))
        self.assertEqual(set(df.columns), set(columns))
        df = df.loc[to_date].droplevel('date')
        self.assertFalse(df.empty)
        for i, item in enumerate(df.iterrows()):
            symbol, series = item
            self.assertEqual(
                test_values[i],
                series.tolist(),
                )

        # Test that results are the same across methods used
        self.assertTrue(df1.iloc[-3:].equals(df2.iloc[-3:]))

    def test_get_dividends(self):
        """ Get historical data for a list of securities. """
        # Test data
        to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d')
        index_names = ['date', 'ticker', 'exchange']
        columns = [
            'date', 'ticker', 'exchange',
            'currency', 'declarationDate', 'paymentDate', 'period',
            'recordDate', 'unadjustedValue', 'value']
        test_df = pd.DataFrame([  # Last 3 dividends
            ['2020-10-21', 'STX40', 'JSE', 'ZAC',         None,         None,        None,         None, 9.1925, 9.1925],
            ['2020-11-06', 'AAPL',   'US', 'USD', '2020-10-29', '2020-11-12', 'Quarterly', '2020-11-09', 0.2050, 0.2050],
            ['2020-11-30', 'MCD',    'US', 'USD', '2020-10-08', '2020-12-15', 'Quarterly', '2020-12-01', 1.2900, 1.2900]],
            columns=columns)

        # Longer date range test causes a decision to use the EOD API service
        from_date1 = '2020-01-01'
        df = self.bulk.get_dividends(self.symbol_list, from_date1, to_date)
        # Test
        self.assertEqual(len(df), 12)
        self.assertEqual(list(df.index.names), list(index_names))
        df.reset_index(inplace=True)
        df = df[columns]
        self.assertEqual(list(df.columns), list(columns))
        # Test against last 3 dividends
        df = df.iloc[-3:].reset_index(drop=True)  # Make index 0, 1, 2
        date_to_str(df)  # Convert Timestamps
        date_to_str(test_df)  # Convert Timestamps
        pd.testing.assert_frame_equal(df, test_df)

        # Shorter date range test causes a decision to use the Bulk API service
        # In case EOD fixes the bulk dividends API, see tested method docstring
        # from_date2 = '2020-12-25'
        # df = self.bulk.get_dividends(self.symbol_list, from_date2, to_date)
        # df2 = df.copy()
        # # Test
        # self.assertEqual(len(df), 12)
        # self.assertEqual(set(df.index.names), set(index_names))
        # df.reset_index(inplace=True)
        # self.assertEqual(set(df.columns), set(columns))
        # # Test against last 3 dividends
        # df = df.iloc[-3:].reset_index(drop=True)  # Make index 0, 1, 2
        # date_to_str(df)  # Convert Timestamps
        # self.assertTrue(df.equals(test_df))

        # # Test that results are the same across methods used
        # self.assertTrue(df1.iloc[-N:].equals(df2.iloc[-N:]))

    def test_get_forex(self):
        """ Get daily, EOD historial forex."""
        # Test data
        from_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
        to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d')
        columns = ['close', 'high', 'low', 'open', 'volume']
        index_names = ['date', 'ticker']
        test_values = [
            [0.8215, 0.8216, 0.8128, 0.8139, 3575.0],
            [0.7336, 0.7348, 0.7308, 0.7337, 0.0],
            [1.0, 1.0, 1.0, 1.0, 0.0]
            ]

        # Get
        df = self.bulk.get_forex(self.forex_list, from_date, to_date)
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns='adjusted_close', inplace=True)
        # Test-rank columns
        df = df[columns]
        df = df.iloc[-3:]
        # Test
        self.assertEqual(df.index.names, index_names)
        self.assertEqual(list(df.columns), list(columns))
        self.assertEqual(
            test_values,
            df.values.tolist(),
            )

        def test_bad_ticker(self):
            """Fail for bad ticker but get the rest."""


class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        test_classes = [
            TestAPI,
            TestHistorical,
            TestBulk,
            TestBulkHistorical,
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


if __name__ == '__main__':

    suite = Suite()
    suite.run()
