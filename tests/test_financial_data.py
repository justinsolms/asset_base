#!/usr/bin/env unittest
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Test suite for the financial_feed module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the asset_base module.
The asset_base module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
from io import StringIO
import os
import datetime
import unittest
import pandas as pd

from src.asset_base.common import TestSession
from src.asset_base.financial_data import Dump, Static
from src.asset_base.financial_data import MetaData
from src.asset_base.financial_data import History
from src.asset_base.entity import Currency, Domicile, Exchange
from src.asset_base.asset import Forex, Index, Listed


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
        self.assertEqual(self.feed.get_class_data_path(), "static")

    def test_get_currency(self):
        """Get currency data from local static file."""
        columns = ["ticker", "name", "country_code_list"]
        data = self.feed.get_currency()
        country_code_list = "GB,GG,IM,JE"  # for GPB
        self.assertListEqual(data.columns.tolist(), columns)
        for i, row in data.iterrows():
            item0 = row[columns[0]]
            item1 = row[columns[1]]
            self.assertIsInstance(item0, str)
            self.assertIsInstance(item1, str)
            self.assertEqual(len(item0), 3)
        # Test country code list
        self.assertEqual(
            set(data[data.ticker == "GBP"]["country_code_list"].tolist()[0]),
            set(country_code_list),
        )

    def test_get_domicile(self):
        """Get currency data from local static file."""
        columns = ["country_code", "country_name", "currency_ticker"]
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
        columns = ["country_code", "mic", "exchange_name", "eod_code"]
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


class TestMetaData(unittest.TestCase):
    """Provide fundamental and meta-data of the working universe securities."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        cls.feed = MetaData()

    def setUp(self):
        """Set up test case fixtures."""
        pass

    def test___init__(self):
        """Initialization."""
        self.assertIsInstance(self.feed, MetaData)
        self.assertEqual(self.feed.get_class_data_path(), "static")

    def test_get_etfs(self):
        """Fetch JSE ETF mata-data from a local file."""
        columns_dict = {
            "mic": str,
            "listed_name": str,
            "asset_class": str,
            "domicile_code": str,
            "industry_class": str,
            "industry_code": str,
            "industry_name": str,
            "isin": str,
            "issuer_domicile_code": str,
            "issuer_name": str,
            "locality": str,
            "sector_code": str,
            "sector_name": str,
            "sub_sector_code": str,
            "sub_sector_name": str,
            "super_sector_code": str,
            "super_sector_name": str,
            "ter": float,
            "ticker": str,
            "status": str,
            "quote_units": str,
            "distributions": bool,
        }
        data = self.feed.get_etfs()
        self.assertEqual(set(data.columns.tolist()), set(columns_dict.keys()))

    def test_get_indices(self):
        """Fetch indices form the feeds."""
        # Test data
        test_csv = (
            "index_name,ticker,currency_code\n"
            "S&P 500 Materials (Sector),SP500-15,USD\n"
            "S&P 500 Chemicals,SP500-151010,USD\n"
            "S&P 500 Industrials (Sector),SP500-20,USD\n"
            "S&P 500 Consumer Discretionary (Sector),SP500-25,USD\n"
            "S&P 500 Consumer Staples (Sector),SP500-30,USD\n"
            "S&P 500 Health Care (Sector),SP500-35,USD\n"
            "S&P 500 Financials (Sector),SP500-40,USD\n"
            "S&P 500 Information Technology (Sector),SP500-45,USD\n"
            "S&P 500 Telecommunication Services (Sector),SP500-50,USD\n"
            "S&P 500 Utilities (Sector),SP500-55,USD\n"
            "S&P 500 Real Estate (Sector),SP500-60,USD\n"
            "S&P 500 Bond Index Total,SP500BDT,USD\n"
            "S&P 500 Net Total Return,SP500NTR,USD\n"
            "S&P 500 TR (Total Return),SP500TR,USD\n"
        )
        test_io = StringIO(test_csv)   # Convert String into StringIO
        test_df = pd.read_csv(test_io)
        # Call
        df = self.feed.get_indices()
        df = df[df.ticker.isin(test_df.ticker)]
        # Sort rows by ticker and columns by name
        test_df = test_df.sort_values('ticker').sort_index(axis='columns').reset_index(drop=True)
        df = df.sort_values('ticker').sort_index(axis='columns').reset_index(drop=True)
        # Test
        pd.testing.assert_frame_equal(test_df, df)


class TestSecuritiesHistory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Securities meta-data
        securities_dataframe = MetaData().get_etfs()
        # Securities feed
        cls.feed = History()
        # Select Test security symbol identities subset
        symbols = [("AAPL", "XNYS"), ("MCD", "XNYS"), ("STX40", "XJSE")]
        securities_dataframe.set_index(["ticker", "mic"], inplace=True)
        cls.securities_dataframe = securities_dataframe.loc[symbols]
        cls.securities_dataframe.reset_index(drop=False, inplace=True)
        # Forex tickers
        cls.forex_tickers = ("USDEUR", "USDGBP", "USDUSD")

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean (but persistent) sqlite in-memory database
        self.test_session = TestSession()
        self.session = self.test_session.session
        # Add all initialization objects to asset_base
        static_obj = Static()
        Currency.update_all(self.session, get_method=static_obj.get_currency)
        Domicile.update_all(self.session, get_method=static_obj.get_domicile)
        Exchange.update_all(self.session, get_method=static_obj.get_exchange)

    def test___init__(self):
        """Initialization."""
        self.assertIsInstance(self.feed, History)
        self.assertEqual(self.feed.get_class_data_path(), None)

    def test_get_eod(self):
        """Get historical EOD for a specified list of securities."""
        # Listed securities test instances
        Listed.from_data_frame(self.session, self.securities_dataframe)
        # Securities test instances list
        securities_list = self.session.query(Listed).all()
        # Test data
        date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d").date()
        # Test data
        test_csv = (
            "date_stamp,adjusted_close,close,high,low,open,volume,isin\n"
            "2020-12-31,130.3872,132.69,134.74,131.72,134.08,99116600,US0378331005\n"
            "2020-12-31,201.8414,214.58,214.93,210.78,211.25,2610900,US5801351017\n"
            "2020-12-31,5185.1674,5460.0,5511.0,5403.0,5492.0,112700,ZAE000027108\n"
        )
        test_io = StringIO(test_csv)   # Convert String into StringIO
        test_df = pd.read_csv(test_io)
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        # Call
        df = self.feed.get_eod(securities_list, date, date)
        # Do not test for 'adjusted_close' as it changes
        test_df.drop(columns="adjusted_close", inplace=True)
        df.drop(columns="adjusted_close", inplace=True)
        # Sort rows by ticker and columns by name
        test_df = test_df.sort_values('isin').sort_index(axis='columns')
        df = df.sort_values('isin').sort_index(axis='columns')
        # Reset indices for test
        test_df.reset_index(drop=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        # Test
        pd.testing.assert_frame_equal(test_df, df)

    def test_get_dividends(self):
        """Get historical dividends for a specified list of securities."""
        # Listed securities test instances
        Listed.from_data_frame(self.session, self.securities_dataframe)
        # Securities test instances list
        securities_list = self.session.query(Listed).all()
        # Date range
        from_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d")
        to_date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d")
        # Test data
        test_csv = (
            "date_stamp,currency,declaration_date,payment_date,period,record_date,unadjusted_value,adjusted_value,isin\n"
            "2020-01-15,ZAC,,,,,7.9238,7.9238,ZAE000027108\n"
            "2020-02-07,USD,2020-01-28,2020-02-13,Quarterly,2020-02-10,0.77,0.1925,US0378331005\n"
            "2020-02-28,USD,2020-01-29,2020-03-16,Quarterly,2020-03-02,1.25,1.25,US5801351017\n"
            "2020-04-15,ZAC,,,,,7.1194,7.1194,ZAE000027108\n"
            "2020-05-08,USD,2020-04-30,2020-05-14,Quarterly,2020-05-11,0.82,0.205,US0378331005\n"
            "2020-05-29,USD,2020-05-22,2020-06-15,Quarterly,2020-06-01,1.25,1.25,US5801351017\n"
            "2020-07-15,ZAC,,,,,31.5711,31.5711,ZAE000027108\n"
            "2020-08-07,USD,2020-07-30,2020-08-13,Quarterly,2020-08-10,0.82,0.205,US0378331005\n"
            "2020-08-31,USD,2020-07-21,2020-09-15,Quarterly,2020-09-01,1.25,1.25,US5801351017\n"
            "2020-10-21,ZAC,,,,,9.1925,9.1925,ZAE000027108\n"
            "2020-11-06,USD,2020-10-29,2020-11-12,Quarterly,2020-11-09,0.205,0.205,US0378331005\n"
            "2020-11-30,USD,2020-10-08,2020-12-15,Quarterly,2020-12-01,1.29,1.29,US5801351017\n"
        )
        test_io = StringIO(test_csv)   # Convert String into StringIO
        test_df = pd.read_csv(test_io)
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        test_df['declaration_date'] = pd.to_datetime(test_df['declaration_date'])
        test_df['payment_date'] = pd.to_datetime(test_df['payment_date'])
        test_df['record_date'] = pd.to_datetime(test_df['record_date'])
        # Call
        df = self.feed.get_dividends(securities_list, from_date, to_date)
        # Do not test for `adjusted_value` as it changes
        test_df.drop(columns="adjusted_value", inplace=True)
        df.drop(columns="adjusted_value", inplace=True)
        # Sort rows by ticker and columns by name
        test_df = test_df.sort_values(['isin', 'date_stamp']).sort_index(axis='columns')
        df = df.sort_values(['isin', 'date_stamp']).sort_index(axis='columns')
        # Reset indices for test
        test_df.reset_index(drop=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        # Test
        pd.testing.assert_frame_equal(test_df, df)


    def test_get_forex(self):
        """Get historical EOD for a specified list of securities."""
        # Create Forex instances from Currency instances. do not populate Forex
        # time series yet as that is for the tests.
        Forex.update_all(self.session)
        # Forex test instances list
        forex_list = (
            self.session.query(Forex).filter(Forex.ticker.in_(self.forex_tickers)).all()
        )
        # Dates
        date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d")
        # Test data
        test_csv = (
            "date_stamp,ticker,adjusted_close,close,high,low,open,volume\n"
            "2020-12-31,USDEUR,0.8185,0.8185,0.8191,0.8123,0.8131,89060\n"
            "2020-12-31,USDGBP,0.7311,0.7311,0.7351,0.7307,0.734,152240\n"
            "2020-12-31,USDUSD,1.0,1.0,1.0,1.0,1.0,0\n"
        )
        test_io = StringIO(test_csv)   # Convert String into StringIO
        test_df = pd.read_csv(test_io)
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        # Call
        df = self.feed.get_forex(forex_list, date, date)
        # Do not test for `adjusted_close` as it changes
        test_df.drop(columns="adjusted_close", inplace=True)
        df.drop(columns="adjusted_close", inplace=True)
        # Sort rows by ticker and columns by name
        test_df = test_df.sort_values('ticker').sort_index(axis='columns')
        df = df.sort_values('ticker').sort_index(axis='columns')
        # Reset indices for test
        test_df.reset_index(drop=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        # Test
        pd.testing.assert_frame_equal(test_df, df)

    def test_get_indices(self):
        """Get historical EOD for a specified list of securities."""
        # Small set of test index tickers
        index_tickers = ("SP500-35", "SP500-45", "SP500TR")
        # Create index instances
        Index.update_all(self.session, MetaData().get_indices)
        # Index test instances list
        self.index_list = (
            self.session.query(Index).filter(Index.ticker.in_(index_tickers)).all()
        )
        # Dates
        date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d").date()
        # Test data
        test_csv = (
            "date_stamp,ticker,adjusted_close,close,high,low,open,volume\n"
            "2020-12-31,SP500-35,1324.01,1324.01,1325.21,1305.53,1309.3101,136352800\n"
            "2020-12-31,SP500-45,2291.28,2291.28,2294.5601,2269.8701,2284.47,344958200\n"
            "2020-12-31,SP500TR,7759.3501,7759.3501,7767.1699,7699.0498,7712.2402,0\n"
        )
        test_io = StringIO(test_csv)   # Convert String into StringIO
        test_df = pd.read_csv(test_io)
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        # Call
        df = self.feed.get_indices(self.index_list, date, date)
        # Do not test for 'adjusted_close' as it changes
        test_df.drop(columns="adjusted_close", inplace=True)
        df.drop(columns="adjusted_close", inplace=True)
        # Sort rows by ticker and columns by name
        test_df = test_df.sort_values('ticker').sort_index(axis='columns')
        df = df.sort_values('ticker').sort_index(axis='columns')
        # Reset indices for test
        test_df.reset_index(drop=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        # Test
        pd.testing.assert_frame_equal(test_df, df)


class TestDump(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        data1 = pd.DataFrame.from_dict(
            {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]},
            orient="index",
            columns=["X", "Y", "Z"],
        )
        data2 = pd.DataFrame.from_dict(
            {"A": [11, 12, 13], "B": [14, 15, 16], "C": [17, 18, 19]},
            orient="index",
            columns=["X", "Y", "Z"],
        )
        cls.data_dict = {"data1": data1, "data2": data2}
        cls.data_files = ["data1.pandas.dataframe.pkl", "data2.pandas.dataframe.pkl"]

    def setUp(self):
        """Set up test case fixtures."""
        self.dumper = Dump()
        self.dumper.delete(delete_folder=False)  # Delete contents only

    def tearDown(self):
        """Tear down test case fixtures."""
        self.dumper.delete(delete_folder=True)  # Delete test dump folder

    def test___init__(self):
        """Initialization."""
        path = self.dumper._path()
        self.assertTrue(os.path.isdir(path), "Dump path does not exist.")

    def test_write_read(self):
        """Write and read back a dict of data frames to CSV files."""
        self.dumper.write(self.data_dict)
        # Test if dump files exist
        for file_name in self.data_files:
            path = self.dumper._path(file_name)
            self.assertTrue(os.path.isfile(path), f"Dump path {path} does not exist.")
        # Test read back data
        name_list = self.data_dict.keys()
        dump_dict = self.dumper.read(name_list)
        for key, df_read_data in dump_dict.items():
            df_test_data = self.data_dict[key]
            pd.testing.assert_frame_equal(df_test_data, df_read_data)

    def test_read_fail(self):
        """Fails dump file read."""
        name_list = self.data_dict.keys()
        with self.assertRaises(FileNotFoundError):
            self.dumper.read(name_list)

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
            TestMetaData,
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


if __name__ == "__main__":
    suite = Suite()
    suite.run()
