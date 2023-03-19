#!/usr/bin/env unittest
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Test suite for the ``eod_historical_data`` module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the fundmanage module.
The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
import asyncio
import unittest
import aiounittest

import datetime
import pandas as pd

from fundmanage3.utils import date_to_str

# Classes to be tested
from ..eod_historical_data import APISessionManager, Exchanges
from ..eod_historical_data import Historical
from ..eod_historical_data import Bulk
from ..eod_historical_data import MultiHistorical


class TestAPI(aiounittest.AsyncTestCase):
    """ Direct API query, response and result checking. """

    @classmethod
    def setUpClass(cls):
        """ Set up class test fixtures. """
        domain = 'eodhistoricaldata.com'
        service = '/api/eod'
        ticker1 = 'STX40'
        ticker2 = 'STXIND'
        ticker_bad = 'BADTICKER'
        exchange = 'JSE'

        # Path must append ticker and short exchange code to service
        endpoint1 = '{}/{}.{}'.format(service, ticker1, exchange)
        endpoint2 = '{}/{}.{}'.format(service, ticker2, exchange)
        endpoint_bad = '{}/{}.{}'.format(service, ticker_bad, exchange)

        cls.url1 = f'https://{domain}{endpoint1}'
        cls.url2 = f'https://{domain}{endpoint2}'
        cls.url_bad = f'https://{domain}{endpoint_bad}'

        from_date = '2022-01-01'
        to_date = '2022-01-07'
        cls.params = dict(
            from_date=from_date,
            to_date=to_date,
            fmt='json',  # Default to CSV table. See NOTE in _get_retries!
            period='d',  # Default to daily sampling period
            order='a',  # Default to ascending order
        )
        cls.endpoint1 = endpoint1
        cls.endpoint2 = endpoint2
        cls.endpoint_bad = endpoint_bad

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
        async with APISessionManager() as api:
            self.assertIsInstance(api, APISessionManager)

    async def test_get(self):
        """Get with the possibility of retries to the API."""
        index_names = ['date', 'open', 'high', 'low', 'close']
        async with APISessionManager() as api:
            response = await api.get(self.endpoint1, self.params)
            # Check
            self.assertIsInstance(response, pd.DataFrame)
            self.assertEqual(index_names, response.columns.to_list()[0:5])

    async def test_bad_ticker(self):
        """Fail with ticker not found."""
        with self.assertRaises(Exception) as ex:
            async with APISessionManager() as api:
                await api.get(self.endpoint_bad, self.params)

    def test_runner(self):
        """Get multiple requests tasks in the runner."""
        # Data Gathering awaitable
        index_names = ['date', 'open', 'high', 'low', 'close']

        async def get_results():
            tasks_list = list()
            async with APISessionManager() as api:
                tasks_list.append(api.get(self.endpoint1, self.params))
                tasks_list.append(api.get(self.endpoint2, self.params))
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
            self.assertIsInstance(historical, APISessionManager)

    async def test_get_eod(self):
        """ Get daily, EOD historical over a date range. """
        # Test data
        from_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
        to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d')
        columns = ['open', 'high', 'low', 'close', 'volume']
        index = ['date']
        # NOTE: This data may change as EOD historical make corrections
        values = [134.08, 134.74, 131.72, 132.69, 99116600.0]
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
            self.assertIsInstance(bulk, APISessionManager)

    async def test_get_eod(self):
        """ Get bulk EOD price and volume for the exchange on a date. """
        columns = ['open', 'high', 'low', 'close', 'adjusted_close', 'volume',
                   'prev_close', 'change', 'change_p']
        index = ['date', 'ticker', 'exchange']
        date = datetime.datetime.strptime('2021-01-03', '%Y-%m-%d')
        # NOTE: This data may change as EOD historical make corrections
        data = [134.08, 134.74, 131.72, 132.69, 131.516,
                99116600.0, 133.72, -1.03, -0.7703]
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


class TestExchanges(unittest.TestCase):
    """Get exchanges (and list of indices) data."""

    @classmethod
    def setUpClass(cls):
        """ Set up class test fixtures. """
        cls.exchanges = Exchanges()
        cls.exchange = 'JSE'

    def setUp(self):
        """ Set up one test. """
        pass

    def test_get_exchanges(self):
        """Get the full list of supported exchanges."""
        test_columns = [
            'Name', 'Code', 'OperatingMIC', 'Country', 'Currency',
            'CountryISO2', 'CountryISO3']
        test_row = [
            'USA Stocks', 'US', 'XNAS, XNYS', 'USA', 'USD', 'US','USA']
        table = self.exchanges.get_exchanges()
        self.assertEqual(test_columns, table.columns.tolist())
        self.assertEqual(
            test_row, table[table['Name'] == 'USA Stocks'].values.tolist()[0])

    def test_get_exchange_symbol_list(self):
        """Get the full list symbols (tickers) on the exchange."""
        test_columns = [
            'Code', 'Name', 'Country', 'Exchange', 'Currency', 'Type', 'Isin']
        test_row = [
            'WHL', 'Woolworths Holdings Ltd', 'South Africa', 'JSE', 'ZAC',
            'Common Stock', 'ZAE000063863']
        table = self.exchanges.get_exchange_symbols(self.exchange)
        self.assertEqual(test_columns, table.columns.tolist())
        self.assertEqual(
            test_row, table[table['Code'] == 'WHL'].values.tolist()[0])

    def test_get_indices_list(self):
        """Get a list of supported indices."""
        test_columns = [
            'Code', 'Name', 'Country', 'Exchange', 'Currency', 'Type', 'Isin']
        test_row = [
            'J200', 'FTSE/JSE Top 40', 'South Africa', 'INDX', 'ZAR',
            'INDEX', None]
        table = self.exchanges.get_indices()
        self.assertEqual(test_columns, table.columns.tolist())
        self.assertEqual(
            test_row, table[table['Code'] == 'J200'].values.tolist()[0])


class TestMultiHistorical(unittest.TestCase):
    """ Get bulk histories across exchanges, securities and date ranges."""

    @classmethod
    def setUpClass(cls):
        """ Set up class test fixtures. """
        cls.historical = MultiHistorical()
        cls.symbol_list = (('AAPL', 'US'), ('MCD', 'US'), ('STX40', 'JSE'))
        cls.forex_list = ('USDEUR', 'USDGBP', 'USDUSD')
        cls.index_list = ('GSPC', 'ASX', 'J200')
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
        self.assertIsInstance(self.historical, MultiHistorical)

    def test__get_eod(self):
        """ Get historical data for a list of securities. """
        # Test data
        from_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
        to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d')
        index_names = ['date', 'ticker', 'exchange']
        columns = ['close', 'high', 'low', 'open', 'volume']
        # NOTE: This data may change as EOD historical make corrections
        test_values = [
            [132.69, 134.74, 131.72, 134.08, 99116600.0],
            [214.58, 214.93, 210.78, 211.25, 2610900.0],
            [5460.0, 5511.0, 5403.0, 5492.0, 112700.0]
        ]
        # Get
        # Use EOD API
        symbol_list = [
            (s[0], s[1], from_date, to_date) for s in self.symbol_list]
        df = asyncio.run(
            self.historical._get_eod(Historical._historical_eod, symbol_list)
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
            [-1.15, -0.8527, 133.72, 135.99, 133.4, 135.58, 134.87, 96452100.0],
            [-1.15, -0.5406, 211.56, 213.36, 211.28, 212.96, 212.71, 1855000.0],
            [38.0, 0.6983, 5480.0, 5510.0, 5385.0, 5405.0, 5442.0, 57423.0],
        ]
        # Get Bulk EOD (Type=None)
        df = asyncio.run(
            self.historical._get_bulk(
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
        to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d')
        index_names = ['date', 'ticker', 'exchange']
        columns = ['close', 'high', 'low', 'open', 'volume']
        test_values = [  # Last date data
            [132.69, 134.74, 131.72, 134.08, 99116600.0],
            [214.58, 214.93, 210.78, 211.25, 2610900.0],
            [5460.0, 5511.0, 5403.0, 5492.0, 112700.0]
        ]

        # Longer date range test causes a decision to use the EOD API service
        from_date1 = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
        symbol_list_bad = [
            (s[0], s[1], from_date1, to_date) for s in self.symbol_list_bad]
        df = self.historical.get_eod(symbol_list_bad)
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
        df = df.loc[to_date]
        self.assertFalse(df.empty)
        for i, item in enumerate(df.iterrows()):
            symbol, series = item
            self.assertEqual(
                test_values[i],
                series.tolist(),
                )

        # Shorter date range test causes a decision to use the Bulk API service
        from_date2 = datetime.datetime.strptime('2020-12-25', '%Y-%m-%d')
        symbol_list = [
            (s[0], s[1], from_date2, to_date) for s in self.symbol_list]
        df = self.historical.get_eod(symbol_list)
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns='adjusted_close', inplace=True)
        # Test-rank columns
        df = df[columns]
        df2 = df.copy()
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
        from_date1 = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
        symbol_list = [
            (s[0], s[1], from_date1, to_date) for s in self.symbol_list]
        df = self.historical.get_dividends(symbol_list)
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
            [0.8185, 0.8191, 0.8123, 0.8131, 89060.0],
            [0.7311, 0.7351, 0.7307, 0.7340, 0.0],
            [1.0, 1.0, 1.0, 1.0, 0.0]
            ]

        # Get
        forex_list = [
            (s, from_date, to_date) for s in self.forex_list]
        df = self.historical.get_forex(forex_list)
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

    def test_get_index(self):
        """ Get daily, EOD historial forex."""
        # Test data
        from_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')
        to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d')
        columns = ['close', 'high', 'low', 'open', 'volume']
        index_names = ['date', 'ticker']
        test_values = [
            [3673.63, 3723.98, 3664.69, 3723.98, 49334000.0],
            [3756.0701, 3760.2, 3726.8799, 3733.27, 3172510000.0],
            [54379.58, 54615.33, 53932.88, 54615.33, 0.0]]

        # Get
        index_list = [
            (s, from_date, to_date) for s in self.index_list]
        df = self.historical.get_index(index_list)
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


if __name__ == '__main__':

    suite = Suite()
    suite.run()
