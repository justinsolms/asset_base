#!/usr/bin/env unittest
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Test suite for the financial_feed module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the fundmanage module.
The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
import unittest
import os
import pandas as pd

from asset_base.financial_data import Dump, Static
from asset_base.financial_data import SecuritiesFundamentals
from asset_base.financial_data import SecuritiesHistory
from asset_base.financial_data import ForexHistory
from asset_base.entity import Domicile
from asset_base.asset import Currency
from asset_base.asset import Exchange
from asset_base.asset import Listed
from asset_base.asset_base import AssetBase
from fundmanage.utils import date_to_str


class TestStatic(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        cls.feed = Static()

    def setUp(self):
        """Set up test case fixtures."""
        pass

    def test___init__(self):
        """Initialization."""
        self.assertIsInstance(self.feed, Static)
        self.assertEqual(self.feed._data_path, 'data')
        self.assertEqual(self.feed._sub_path, 'static')

    def test_get_currency(self):
        """Get currency data from local static file."""
        columns = ['currency_code', 'currency_name']
        data = self.feed.get_currency()
        self.assertListEqual(data.columns.tolist(), columns)
        for i, row in data.iterrows():
            item0 = row[columns[0]]
            item1 = row[columns[1]]
            self.assertIsInstance(item0, str)
            self.assertIsInstance(item1, str)
            self.assertEqual(len(item0), 3)

    def test_get_domicile(self):
        """Get currency data from local static file."""
        columns = ['domicile_code', 'domicile_name', 'currency_code']
        data = self.feed.get_domicile()
        self.assertEqual(set(data.columns.tolist()), set(columns))
        for i, row in data.iterrows():
            item0 = row[columns[0]]
            item1 = row[columns[1]]
            item2 = row[columns[2]]
            self.assertIsInstance(item0, str)
            self.assertIsInstance(item1, str)
            self.assertIsInstance(item2, str)
            self.assertEqual(len(item0), 2)
            self.assertEqual(len(item2), 3)

    def test_get_exchange(self):
        """Get currency data from local static file."""
        columns = ['domicile_code', 'mic', 'exchange_name', 'eod_code']
        data = self.feed.get_exchange()
        self.assertEqual(set(data.columns.tolist()), set(columns))
        for i, row in data.iterrows():
            item0 = row[columns[0]]
            item1 = row[columns[1]]
            item2 = row[columns[2]]
            self.assertIsInstance(item0, str)
            self.assertIsInstance(item1, str)
            self.assertIsInstance(item2, str)
            self.assertEqual(len(item0), 2)
            self.assertEqual(len(item1), 4)


class TestSecuritiesFundamentals(unittest.TestCase):
    """Provide fundamental and meta-data of the working universe securities."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        cls.feed = SecuritiesFundamentals()

    def setUp(self):
        """Set up test case fixtures."""
        pass

    def test___init__(self):
        """Initialization."""
        self.assertIsInstance(self.feed, SecuritiesFundamentals)
        self.assertEqual(self.feed._data_path, 'data')
        self.assertEqual(self.feed._sub_path, 'static')

    def test_get_securities(self):
        """Fetch JSE securities mata-data from a local file."""
        columns_dict = {
            'mic': str, 'listed_name': str, 'asset_class': str, 'domicile_code': str,
            'exchange_board': str, 'industry_class': str, 'industry_code': str,
            'industry_name': str, 'isin': str, 'issuer_domicile_code': str,
            'issuer_name': str, 'locality': str, 'alt_name': str, 'roll_up': str,
            'sector_code': str, 'sector_name': str,
            'sub_sector_code': str, 'sub_sector_name': str,
            'super_sector_code': str, 'super_sector_name': str, 'ter': float,
            'ticker': str, 'status': str}
        data = self.feed.get_securities()
        self.assertEqual(set(data.columns.tolist()), set(columns_dict.keys()))


class TestSecuritiesHistory(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Securities meta-data
        cls.get_method = SecuritiesFundamentals().get_securities
        securities_dataframe = cls.get_method()
        # Securities feed
        cls.feed = SecuritiesHistory()
        # Select Test security symbol identities subset
        symbols = [('AAPL', 'XNYS'), ('MCD', 'XNYS'), ('STX40', 'XJSE')]
        securities_dataframe.set_index(['ticker', 'mic'], inplace=True)
        cls.securities_dataframe = securities_dataframe.loc[symbols]
        cls.securities_dataframe.reset_index(drop=False, inplace=True)

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean entitybase
        self.entitybase = AssetBase(dialect='memory')
        self.session = self.entitybase.session
        # Add all initialization objects to entitybase
        static_obj = Static()
        Currency.update_all(self.session, get_method=static_obj.get_currency)
        Domicile.update_all(self.session, get_method=static_obj.get_domicile)
        Exchange.update_all(self.session, get_method=static_obj.get_exchange)
        Listed.from_data_frame(self.session, self.securities_dataframe)
        # Securities entitybase instances list
        self.securities_list = self.session.query(Listed).all()

    def test___init__(self):
        """Initialization."""
        self.assertIsInstance(self.feed, SecuritiesHistory)
        self.assertEqual(self.feed._data_path, None)
        self.assertEqual(self.feed._sub_path, None)

    def test_get_eod(self):
        """Get historical EOD for a specified list of securities."""
        # Test data
        from_date = '2020-01-01'
        to_date = '2020-12-31'
        columns = ['date_stamp', 'close', 'high', 'low', 'open', 'volume',
                   'isin']
        # NOTE: This data may change as EOD historical make corrections
        test_df = pd.DataFrame([  # Last date data
            ['2020-12-31',  132.69,  134.74,  131.72,  134.08,  99116594, 'US0378331005'],
            ['2020-12-31',  214.58,  214.93,  210.78,  211.25,   2610914, 'US5801351017'],
            ['2020-12-31', 5460.00, 5511.00, 5403.00, 5492.00,    112700, 'ZAE000027108'],
        ], columns=columns)
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        # Call
        df = self.feed.get_eod(self.securities_list, from_date, to_date)
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns='adjusted_close', inplace=True)
        # Test
        self.assertEqual(len(df), 758)
        # Test against last date data
        self.assertFalse(df.empty)
        df = df.iloc[-3:].reset_index(drop=True)  # Make index 0, 1, 2
        pd.testing.assert_frame_equal(df, test_df)

    def test_get_dividends(self):
        """Get historical dividends for a specified list of securities."""
        # Test data
        to_date = '2020-12-31'
        columns = ['date_stamp', 'currency',
                   'declaration_date', 'payment_date', 'period', 'record_date',
                   'unadjusted_value', 'adjusted_value', 'isin']
        test_df = pd.DataFrame([  # Last 3 dividends
            ['2020-10-21', 'ZAC',         None,         None,        None,         None, 9.1925, 9.1925, 'ZAE000027108'],
            ['2020-11-06', 'USD', '2020-10-29', '2020-11-12', 'Quarterly', '2020-11-09', 0.2050, 0.2050, 'US0378331005'],
            ['2020-11-30', 'USD', '2020-10-08', '2020-12-15', 'Quarterly', '2020-12-01', 1.2900, 1.2900, 'US5801351017'],
        ], columns=columns)
        # Longer date range test causes a decision to use the EOD API service
        from_date1 = '2020-01-01'
        df = self.feed.get_dividends(self.securities_list, from_date1, to_date)
        # Test
        self.assertEqual(len(df), 12)
        df.reset_index(inplace=True, drop=True)
        # Test against last 3 dividends
        df = df.iloc[-3:].reset_index(drop=True)  # Make index 0, 1, 2
        date_to_str(df)  # Convert Timestamps
        date_to_str(test_df)  # Convert Timestamps
        pd.testing.assert_frame_equal(df, test_df)


class TestForexHistory(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Securities meta-data
        cls.get_method = SecuritiesFundamentals().get_securities
        # Securities feed
        cls.feed = ForexHistory()
        # Select Test security symbol identities subset
        cls.forex_list = ('USDEUR', 'USDGBP', 'USDUSD')

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean entitybase
        self.entitybase = AssetBase(dialect='memory')
        self.session = self.entitybase.session
        # Add all initialization objects to entitybase
        static_obj = Static()
        Currency.update_all(self.session, get_method=static_obj.get_currency)
        Domicile.update_all(self.session, get_method=static_obj.get_domicile)
        Exchange.update_all(self.session, get_method=static_obj.get_exchange)
        Listed.from_data_frame(self.session, self.securities_dataframe)
        # Securities entitybase instances list
        self.securities_list = self.session.query(Listed).all()

    def test___init__(self):
        """Initialization."""
        self.assertIsInstance(self.feed, ForexHistory)
        self.assertEqual(self.feed._data_path, None)
        self.assertEqual(self.feed._sub_path, None)

    def test_get_eod(self):
        """Get historical EOD for a specified list of securities."""
        # FIXME: This is not a Forex test but a copied EOD test!!!!!
        # Test data
        from_date = '2020-01-01'
        to_date = '2020-12-31'
        columns = [
            'date_stamp', 'ticker', 'mic', 'isin',
            'close', 'high', 'low', 'open', 'volume']
        test_columns = ['close', 'high', 'low', 'open', 'volume']
        # NOTE: This data may change as EOD historical make corrections
        test_values = [  # Last date data
            [132.69, 134.74, 131.72, 134.08, 99116594.0],
            [214.58, 214.93, 210.78, 211.25, 2610914.0],
            [5460.0, 5511.0, 5403.0, 5492.0, 112700.0]
        ]
        # Call
        df = self.feed.get_eod(self.securities_list, from_date, to_date)
        # Do not test for 'adjusted_close' as it changes
        df.drop(columns='adjusted_close', inplace=True)
        # Test
        self.assertEqual(len(df), 756)
        self.assertEqual(set(df.columns), set(columns))
        # Test against last date data
        df = df[df['date_stamp'] == to_date][test_columns]
        self.assertFalse(df.empty)
        for i, item in enumerate(df.iterrows()):
            symbol, series = item
            self.assertEqual(series.tolist(), test_values[i])


class TestDump(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        data1 = pd.DataFrame.from_dict(
            {'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]},
            orient='index', columns=['X', 'Y', 'Z'])
        data2 = pd.DataFrame.from_dict(
            {'A': [11, 12, 13], 'B': [14, 15, 16], 'C': [17, 18, 19]},
            orient='index', columns=['X', 'Y', 'Z'])
        cls.data_dict = {'data1': data1, 'data2': data2}
        cls.data_files = [
            'data1.pandas.dataframe.pkl', 'data2.pandas.dataframe.pkl']

    def setUp(self):
        """Set up test case fixtures."""
        self.dumper = Dump()

    def test___init__(self):
        """Initialization."""
        path = self.dumper._path()
        self.assertTrue(os.path.isdir(path), 'Dump path does not exist.')

    def test_write_read(self):
        """Write and read back a dict of data frames to CSV files."""
        self.dumper.write(self.data_dict)
        # Test if dump files exist
        for file_name in self.data_files:
            path = self.dumper._path(file_name)
            self.assertTrue(
                os.path.isfile(path), f'Dump path {path} does not exist.')
        # Test read back data
        name_list = self.data_dict.keys()
        dump_dict = self.dumper.read(name_list)
        for key, df_read_data in dump_dict.items():
            df_test_data = self.data_dict[key]
            pd.testing.assert_frame_equal(df_test_data, df_read_data)

    def test_delete(self):
        """Delete the dump folder and its contents."""
        self.dumper.write(self.data_dict)
        self.dumper.delete()


class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # Classes that are passing. Add the others later when they too work.
        test_classes = [
            TestStatic,
            TestSecuritiesFundamentals,
            TestSecuritiesHistory,
            TestDump,
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
