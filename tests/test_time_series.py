from io import StringIO
from os import close
import unittest
import datetime
from attrs import Factory
import pandas as pd

from pygments import highlight
from sqlalchemy.exc import IntegrityError

from src.asset_base.common import TestSession
from src.asset_base.financial_data import Dump, MetaData, History, Static
from src.asset_base.entity import Currency, Domicile, Issuer, Exchange
from src.asset_base.asset import Asset, AssetBase, Forex, Index, Listed, ListedEquity, Share
from src.asset_base.time_series import Dividend, ForexEOD, IndexEOD, SimpleEOD
from src.asset_base.time_series import TimeSeriesBase, ListedEOD, TradeEOD


class TestTimeSeriesBase(unittest.TestCase):
    """ "Common time-series ORM capability."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = TimeSeriesBase
        # Today
        cls.today = datetime.date.today()
        # Similar set up to test_financial_data
        # Securities meta-data
        cls.get_method = MetaData().get_etfs
        cls.securities_dataframe = cls.get_method()
        # Securities feed
        cls.feed = History()

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean (but persistent) sqlite in-memory database
        self.test_session = TestSession()
        self.session = self.test_session.session
        # Add all initialization objects to asset_base
        static = Static()
        Currency.update_all(self.session, get_method=static.get_currency)
        Domicile.update_all(self.session, get_method=static.get_domicile)
        Exchange.update_all(self.session, get_method=static.get_exchange)
        # Get fixture instances
        self.currency = Currency.factory(self.session, "USD")

    def tearDown(self) -> None:
        """Tear down test case fixtures."""
        del self.test_session

    def test___init__(self):
        """Test class relationship and polymorphism functionality."""
        # Create an minimal args asset and a time series instance
        asset = AssetBase(self.name, self.currency)
        ts_item = TimeSeriesBase(asset, date_stamp=self.today)
        # Test adding them to the database
        self.session.add(asset)
        self.session.add(ts_item)
        self.session.flush()
        # Test the relationship between them
        self.assertEqual(asset, ts_item.base_obj)
        # Test attributes
        self.assertEqual(ts_item.date_stamp, self.today)

    def test_unique_constraint(self):
        """Fail the UniqueConstraint('_listed_id', 'date_stamp')"""
        # Create an asset and two time series items with the same date_stamp
        # thus violating the UniqueConstraint
        asset = AssetBase(self.name, self.currency)
        ts_item1 = TimeSeriesBase(asset, date_stamp=self.today)
        ts_item2 = TimeSeriesBase(asset, date_stamp=self.today)
        self.session.add(asset)
        self.session.add(ts_item1)
        self.session.add(ts_item2)
        with self.assertRaises(IntegrityError):
            self.session.flush()


class TestSimpleEOD(TestTimeSeriesBase):
    """Test an asset's date-stamped ``SimpleEOD`` trade data item.

    Use an ``Asset`` and a ``SimpleEOD`` time series item with the asset's day
    price.
    """

    def test___init__(self):
        """Test class relationship and polymorphism functionality."""
        # Fixture
        close = 123.45
        # Create an minimal args asset and a time series instance
        asset = Asset(self.name, self.currency)
        ts_item = SimpleEOD(asset, date_stamp=self.today, close=close)
        # Test adding them to the database
        self.session.add(asset)
        self.session.add(ts_item)
        self.session.flush()
        # Test the relationship between them
        self.assertEqual(asset, ts_item.base_obj)
        # Query them back
        asset = self.session.query(Asset).one()
        ts_item = self.session.query(SimpleEOD).one()
        # Test the relationship between them
        self.assertEqual(asset, ts_item.base_obj)
        # Test attributes
        self.assertEqual(ts_item.date_stamp, self.today)
        self.assertEqual(ts_item._close, close)


class TestTradeEOD(TestTimeSeriesBase):
    """Test a share's date-stamped ``TradeEOD`` trade data item.

    Use an ``Asset`` and a ``TradeEOD`` time series item with the shares's day
    trade data.
    """

    def to_dict(self, item):
        """Convert all class price attributes to a dictionary."""
        data = item.to_dict()
        data.update(
            isin=item.listed_equity.isin,
            ticker=item.listed_equity.ticker,
            mic=item.listed_equity.exchange.mic,
        )
        return data

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Share fixtures
        cls.issuer_name = "ABC Inc"
        cls.issuer_domicile_code = "US"
        cls.name = "ABC Share"
        # Trade data
        cls.Cls = TradeEOD
        cls.open = 123.0
        cls.close = 123.1
        cls.high = 123.2
        cls.low = 123.3
        cls.adjusted_close = 123.4
        cls.volume = 1000

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        self.issuer = Issuer.factory(self.session, self.issuer_name, self.issuer_domicile_code)

    def test___init__(self):
        """Test class relationship and polymorphism functionality."""
        # Create an minimal args asset and a time series instance
        share = Share(self.name, self.issuer)
        ts_item = TradeEOD(
            share, date_stamp=self.today, open=self.open, close=self.close,
            high=self.high, low=self.low, adjusted_close=self.adjusted_close,
            volume=self.volume)
        # Test adding them to the database
        self.session.add(share)
        self.session.add(ts_item)
        self.session.flush()
        # Test the relationship between them
        self.assertEqual(share, ts_item.base_obj)
        # Query them back
        share = self.session.query(Asset).one()
        ts_item = self.session.query(SimpleEOD).one()
        # Test the relationship between them
        self.assertEqual(share, ts_item.base_obj)
        # Test data items and to_dict method
        trade_data = ts_item.to_dict()
        self.assertEqual(trade_data['date_stamp'], self.today)
        self.assertEqual(trade_data['open'], self.open)
        self.assertEqual(trade_data['close'], self.close)
        self.assertEqual(trade_data['high'], self.high)
        self.assertEqual(trade_data['low'], self.low)
        self.assertEqual(trade_data['adjusted_close'], self.adjusted_close)
        self.assertEqual(trade_data['volume'], self.volume)


class TestForexEOD(TestTradeEOD):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Forex

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Currencies for Forex tests
        self.base_currency = Currency.factory(self.session, Forex.root_currency_ticker)
        self.price_currency = Currency.factory( self.session, Forex.foreign_currencies[0])  # Pick first foreign currency

    def test___init__(self):
        """Test class relationship and polymorphism functionality."""
        # Create an minimal args asset and a time series instance
        forex = Forex.factory(self.session, self.base_currency.ticker, self.price_currency.ticker)
        ts_item = TradeEOD(
            forex, date_stamp=self.today, open=self.open, close=self.close,
            high=self.high, low=self.low, adjusted_close=self.adjusted_close,
            volume=self.volume)
        # Test adding them to the database
        self.session.add(forex)
        self.session.add(ts_item)
        self.session.flush()
        # Test the relationship between them
        self.assertEqual(forex, ts_item.base_obj)
        # Query them back
        forex = self.session.query(Asset).one()
        ts_item = self.session.query(SimpleEOD).one()
        # Test the relationship between them
        self.assertEqual(forex, ts_item.base_obj)
        # Test data items and to_dict method
        trade_data = ts_item.to_dict()
        self.assertEqual(trade_data['date_stamp'], self.today)
        self.assertEqual(trade_data['open'], self.open)
        self.assertEqual(trade_data['close'], self.close)
        self.assertEqual(trade_data['high'], self.high)
        self.assertEqual(trade_data['low'], self.low)
        self.assertEqual(trade_data['adjusted_close'], self.adjusted_close)
        self.assertEqual(trade_data['volume'], self.volume)

# Skip this for now
@unittest.skip("IndexEOD not deployed yet.")
class TestIndexEOD(TestTradeEOD):
    pass

class TestListedEOD(TestTradeEOD):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = ListedEOD
        # Listed argument fixtures
        cls.isin = "US88579Y1010"
        cls.ticker = "ABC"
        cls.status = "listed"  # Must be listed to be updatable
        # Exchange
        cls.mic = "XNYS"
        # Time series test data
        test_csv = (
            "date_stamp,adjusted_close,close,high,low,open,volume,isin\n"
            f"2020-12-01,123.0,123.1,123.2,123.3,123.4,1000,{cls.isin}\n"
            f"2020-12-02,124.0,124.1,124.2,124.3,124.4,1001,{cls.isin}\n"
            f"2020-12-03,125.0,125.1,125.2,125.3,125.4,1002,{cls.isin}\n"
        )
        # Convert String into StringIO
        test_df = pd.read_csv(StringIO(test_csv))
        # Convert date_stamp to pandas datetime
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        cls.test_df = test_df

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # US Domicile
        self.domicile = Domicile.factory(self.session, "US")
        self.exchange = Exchange.factory(self.session, self.mic)

    def test_data_frame(self):
        """Test from and to a pandas trade time-series DataFrame.

        Its or meaningful to test ths at this level as TradeEOD is rich in data
        fields and testing at parent levels would be less effective and testing
        at child levels would be more complex. So a ``Share`` test seems right.
        """
        # Create an minimal args Listed and a time series instance
        listed = Listed(self.name, self.issuer, self.isin, self.exchange, self.ticker, self.status)
        self.session.add(listed)
        self.session.flush()
        # Create time series items from test DataFrame
        ListedEOD.from_data_frame(self.session, Listed, data_frame=self.test_df)
        # Query the listed back them back
        listed = self.session.query(Asset).one()
        # Get all time series items back
        df = ListedEOD.to_data_frame(self.session, Listed)
        # Test DataFrames
        pd.testing.assert_frame_equal(df.sort_index(axis=1), self.test_df.sort_index(axis=1))

    def test_0_dump(self):
        """Dump all class instances and their time series data to disk."""
        # Dumper
        dumper = Dump(testing=True)
        # For testing delete old any test dump folder and re-create it empty
        dumper.delete(delete_folder=True)  # Delete dump folder and contents
        dumper.makedir()
        # This test is stolen from test_financial_data
        # Call API for data
        test_df = self.feed.get_eod(self.securities_list, self.from_date, self.to_date)
        # Call the tested method.
        ListedEOD.from_data_frame(self.session, Listed, data_frame=test_df)
        # Dump methods to be tested
        ListedEOD.dump(self.session, dumper, Listed)
        # Verify dump file exists.
        self.assertTrue(dumper.exists(ListedEOD), "ListedEOD dump file not found.")


class TestDividend(TestTimeSeriesBase):
    """A single listed security's date-stamped EOD trade data."""

    # datetime to date-string converter
    def date_to_str(self, df):
        """Convert Timestamp objects to test date-strings."""
        # Replace pesky pandas NaT with None
        df.replace({pd.NaT: None}, inplace=True)
        for index, row in df.iterrows():
            for column, item in row.items():
                if (
                    isinstance(item, pd.Timestamp)
                    or isinstance(item, datetime.date)
                    or isinstance(item, datetime.datetime)
                ):
                    row[column] = item.strftime("%Y-%m-%d")
            df.loc[index] = row

    def to_dict(self, item):
        """Convert all class price attributes to a dictionary."""
        data = item.to_dict()
        data.update(
            isin=item.listed_equity.isin,
            ticker=item.listed_equity.ticker,
            mic=item.listed_equity.exchange.mic,
        )
        return data

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Dividend

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Add *ALL* Listed security instances
        ListedEquity.from_data_frame(self.session, self.securities_dataframe)
        # Securities asset_base instances list
        self.securities_list = self.session.query(ListedEquity).all()
        # Date range
        self.from_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d").date()
        self.to_date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d").date()
        # Test data
        test_csv = (
            "date_stamp,currency,declaration_date,payment_date,period,record_date,unadjusted_value,adjusted_value,isin\n"
            "2020-01-15,ZAC,,,,,0.079238,0.079238,ZAE000027108\n"
            "2020-02-07,USD,2020-01-28,2020-02-13,Quarterly,2020-02-10,0.77,0.1925,US0378331005\n"
            "2020-02-28,USD,2020-01-29,2020-03-16,Quarterly,2020-03-02,1.25,1.25,US5801351017\n"
            "2020-04-15,ZAC,,,,,0.071194,0.071194,ZAE000027108\n"
            "2020-05-08,USD,2020-04-30,2020-05-14,Quarterly,2020-05-11,0.82,0.205,US0378331005\n"
            "2020-05-29,USD,2020-05-22,2020-06-15,Quarterly,2020-06-01,1.25,1.25,US5801351017\n"
            "2020-07-15,ZAC,,,,,0.315711,0.315711,ZAE000027108\n"
            "2020-08-07,USD,2020-07-30,2020-08-13,Quarterly,2020-08-10,0.82,0.205,US0378331005\n"
            "2020-08-31,USD,2020-07-21,2020-09-15,Quarterly,2020-09-01,1.25,1.25,US5801351017\n"
            "2020-10-21,ZAC,,,,,0.091925,0.091925,ZAE000027108\n"
            "2020-11-06,USD,2020-10-29,2020-11-12,Quarterly,2020-11-09,0.205,0.205,US0378331005\n"
            "2020-11-30,USD,2020-10-08,2020-12-15,Quarterly,2020-12-01,1.29,1.29,US5801351017\n"
        )
        test_io = StringIO(test_csv)   # Convert String into StringIO
        test_df = pd.read_csv(test_io)
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        test_df['declaration_date'] = pd.to_datetime(test_df['declaration_date'])
        test_df['payment_date'] = pd.to_datetime(test_df['payment_date'])
        test_df['record_date'] = pd.to_datetime(test_df['record_date'])
        self.test_df = test_df

    def test___init__(self):
        """Initialization."""
        # Get AAPL Inc instance form committed instances. This is different
        # from the inherited `self.listed` instance; which it must override
        # here.
        listed_equity = ListedEquity.factory(self.session, isin=self.isin)
        # Test for AAPL Inc.
        dividend = Dividend(
            listed_equity,
            date_stamp=datetime.date.today(),
            currency="ZAR",
            declaration_date=datetime.date.today(),
            payment_date=datetime.date.today(),
            period="Quarterly",
            record_date=datetime.date.today(),
            unadjusted_value=1.0,
            adjusted_value=1.01,
        )
        self.session.add(dividend)
        self.assertIsInstance(dividend, Dividend)
        self.assertEqual(dividend.base_obj, listed_equity)
        self.assertEqual(dividend.date_stamp, datetime.date.today())
        self.assertEqual(dividend.currency, "ZAR")
        self.assertEqual(dividend.declaration_date, datetime.date.today())
        self.assertEqual(dividend.payment_date, datetime.date.today())
        self.assertEqual(dividend.period, "Quarterly")
        self.assertEqual(dividend.record_date, datetime.date.today())
        self.assertEqual(dividend._unadjusted_value, 1.0)
        self.assertEqual(dividend._adjusted_value, 1.01)

        # Test polymorphism functionality by query of the superclass
        # TimeSeriesBase which should produce a Dividend polymorphic instance
        instance = self.session.query(TimeSeriesBase).one()
        self.assertEqual(instance.class_name, "Dividend")
        self.assertEqual(instance._discriminator, "dividend")
        self.assertEqual(dividend, instance)

        # Test Listed._eod_series <-> ListedEOD.listed relationship
        self.assertEqual(listed_equity._series[0], dividend)
        self.assertEqual(listed_equity._dividend_series[0], dividend)
        self.assertEqual(dividend.listed_equity, listed_equity)

        # Test that ListedEquity._eod_series lists purely ListedEOD instances
        # and ListedEquity._dividend_series lists purely Dividend instances.
        # This requires the addition of a ListedEOD in addition to the Dividend.
        self.assertEqual(len(listed_equity._series), 1)
        listed_eod = ListedEOD(
            listed_equity,
            date_stamp=datetime.date.today(),
            open=1.0,
            close=2.0,
            high=3.0,
            low=4.0,
            adjusted_close=5.0,
            volume=6.0,
        )
        self.session.add(listed_eod)
        # Test Listed._eod_series <-> ListedEOD.listed relationship
        self.assertEqual(len(listed_equity._eod_series), 1)
        self.assertEqual(listed_equity._eod_series[0], listed_eod)
        self.assertEqual(listed_eod.listed, listed_equity)
        # (again) Test Listed._eod_series <-> ListedEOD.listed relationship
        self.assertEqual(len(listed_equity._dividend_series), 1)
        self.assertEqual(listed_equity._dividend_series[0], dividend)
        self.assertEqual(dividend.listed_equity, listed_equity)
        # There should now be two time TimeSeriesBase instances
        self.assertEqual(len(listed_equity._series), 2)

    def test_session_commit(self):
        """Committing to the database."""
        # Get AAPL Inc instance form committed instances. This is different
        # from the inherited `self.listed` instance; which it must override
        # here.
        listed_equity = ListedEquity.factory(self.session, isin=self.isin)
        # Test for AAPL Inc.
        ts_item = Dividend(
            listed_equity,
            date_stamp=datetime.date.today(),
            currency="ZAR",
            declaration_date=datetime.date.today(),
            payment_date=datetime.date.today(),
            period="Quarterly",
            record_date=datetime.date.today(),
            unadjusted_value=1.0,
            adjusted_value=1.01,
        )
        self.session.add(ts_item)
        self.session.commit()
        self.assertEqual(ts_item.base_obj, listed_equity)
        self.assertEqual(ts_item._asset_id, listed_equity.id)

    def test_data_frame(self):
        """Convert all instances to a single data table."""
        test_df = self.test_df
        # Methods tested
        df = self.feed.get_dividends(self.securities_list, self.from_date, self.to_date)
        Dividend.from_data_frame(self.session, ListedEquity, data_frame=df)
        df = Dividend.to_data_frame(self.session, ListedEquity)
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

    def test_update_all(self):
        """Get historical dividends for a specified list of securities."""
        test_df = self.test_df
        # Test stolen from test_financial_data
        # Call the tested method.
        Dividend.update_all(self.session, self.feed.get_dividends)
        # Retrieve the submitted date stamped data from asset_base
        df = pd.DataFrame(
            [self.to_dict(item) for item in self.session.query(Dividend).all()]
        )
        # Test date range
        df = df[(self.from_date <= df.date_stamp.dt.date) & (df.date_stamp.dt.date <= self.to_date)]
        # Drop columns not in test data
        df.drop(columns=["mic", "ticker"], inplace=True)
        # Test over test-date-range
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df['declaration_date'] = pd.to_datetime(df['declaration_date'])
        df['payment_date'] = pd.to_datetime(df['payment_date'])
        df['record_date'] = pd.to_datetime(df['record_date'])
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

    def test_0_dump(self):
        """Dump all class instances and their time series data to disk."""
        # Dumper
        dumper = Dump(testing=True)
        # For testing delete old any test dump folder and re-create it empty
        dumper.delete(delete_folder=True)  # Delete dump folder and contents
        dumper.makedir()
        # This test is stolen from test_financial_data
        # Call API for data
        test_df = self.feed.get_dividends(
            self.securities_list, self.from_date, self.to_date
        )
        # Call the tested method.
        Dividend.from_data_frame(self.session, ListedEquity, data_frame=test_df)
        # Methods to be tested
        Dividend.dump(self.session, dumper, ListedEquity)
        # Verify dump file exists.
        self.assertTrue(dumper.exists(Dividend), "Dividend dump file not found.")


class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # Classes that are passing. Add the others later when they too work.
        test_classes = [
            TestTimeSeriesBase,
            TestSimpleEOD,
            TestListedEOD,
            TestDividend,
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
