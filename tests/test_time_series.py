from io import StringIO
from os import close
import unittest
import datetime
from attrs import Factory
import pandas as pd

from pygments import highlight
from sqlalchemy.exc import IntegrityError, NoResultFound

from src.asset_base.common import TestSession
from src.asset_base.financial_data import Dump, MetaData, History, Static
from src.asset_base.entity import Currency, Domicile, Issuer, Exchange
from src.asset_base.asset import Asset, AssetBase, Forex, Index, Listed, ListedEquity, Share
from src.asset_base.time_series import Dividend, ForexEOD, IndexEOD, EODBase
from src.asset_base.time_series import TimeSeriesBase, ListedEOD, TradeEOD
from tests.test_asset import TestListedEquity


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
        # Asset attributes
        cls.asset_name = "Test Asset"

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
        # Create an minimal args asset and a time series instance
        asset = AssetBase(self.asset_name, self.currency)
        ts_item = TimeSeriesBase(asset, date_stamp=self.today)
        # Test adding them to the database
        self.session.add(asset)
        self.session.add(ts_item)
        self.session.flush()

    def tearDown(self) -> None:
        """Tear down test case fixtures."""
        del self.test_session

    def test___init__(self):
        """Test class relationship and polymorphism functionality."""
        # Due to test inheritance use discriminator to block polymorphs
        asset = self.session.query(AssetBase).filter(AssetBase._discriminator=="asset_base").one()
        ts_item = self.session.query(TimeSeriesBase).one()
        # Test the relationship between them
        self.assertEqual(asset, ts_item._base_obj)
        # Test attributes
        self.assertEqual(ts_item.date_stamp, self.today)

        # Fail the UniqueConstraint('_listed_id', 'date_stamp')
        # Need discriminator due to test inheritance which would return all
        # polymorphs. Overloading this test would remove the need for the filter.
        # Due to test inheritance use discriminator to block polymorphs
        asset = self.session.query(AssetBase).filter(AssetBase._discriminator=="asset_base").one()
        # Add second non-unique time series item
        ts_item2 = TimeSeriesBase(asset, date_stamp=self.today)
        self.session.add(ts_item2)
        with self.assertRaises(IntegrityError):
            self.session.flush()


class TestEODBase(TestTimeSeriesBase):
    """Test an asset's date-stamped ``SimpleEOD`` trade data item.

    Use an ``Asset`` and a ``SimpleEOD`` time series item with the asset's day
    price.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # SimpleEOD fixtures
        cls.Cls = EODBase
        cls.close = 123.45

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Create an minimal args simple EOD time series instance
        asset = Asset(self.asset_name, self.currency)
        ts_item = EODBase(asset, date_stamp=self.today, price=self.close)
        # Test adding ts to the database
        self.session.add(asset)
        self.session.add(ts_item)
        self.session.flush()

    def test___init__(self):
        """Test class relationship and polymorphism functionality."""
        # Due to test inheritance use discriminator to block polymorphs
        asset = self.session.query(Asset).filter(Asset._discriminator=="asset").one()
        ts_item = self.session.query(EODBase).one()
        # Test the relationship between them
        self.assertEqual(asset, ts_item._base_obj)
        # Test attributes
        self.assertEqual(ts_item.date_stamp, self.today)
        self.assertEqual(ts_item.price, self.close)


class TestTradeEOD(TestTimeSeriesBase):
    """Test a share's date-stamped ``TradeEOD`` trade data item.

    Use an ``Share`` and a ``TradeEOD`` time series item with the shares's day
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
        cls.Cls = TradeEOD
        # Share fixtures
        cls.issuer_name = "ABC Inc"
        cls.issuer_domicile_code = "US"
        cls.name = "ABC Share"
        # Trade data
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
        # Create an minimal args Share and a time series instance
        share = Share(self.name, self.issuer)
        ts_item = TradeEOD(
            share, date_stamp=self.today, open=self.open, close=self.close,
            high=self.high, low=self.low, adjusted_close=self.adjusted_close,
            volume=self.volume)
        # Test adding them to the database
        self.session.add(share)
        self.session.add(ts_item)
        self.session.flush()

    def test___init__(self):
        """Test class relationship and polymorphism functionality."""
        # Due to test inheritance use discriminator to block polymorphs
        share = self.session.query(Share).filter(Share._discriminator=="share").one()
        ts_item = self.session.query(TradeEOD).one()
        # Test the relationship between them
        self.assertEqual(share, ts_item._base_obj)
        # Test the relationship between them
        self.assertEqual(share, ts_item._base_obj)
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
        # Create an minimal args Forex EOD time series instance
        forex = Forex.factory(self.session, self.base_currency.ticker, self.price_currency.ticker)
        ts_item = ForexEOD(
            forex, date_stamp=self.today, open=self.open, close=self.close,
            high=self.high, low=self.low, adjusted_close=self.adjusted_close,
            volume=self.volume)
        # Test adding them to the database
        self.session.add(forex)
        self.session.add(ts_item)
        self.session.flush()

    def test___init__(self):
        """Test class relationship and polymorphism functionality."""
        forex = self.session.query(Forex).one()
        ts_item = self.session.query(ForexEOD).one()
        # Test the relationship between them
        self.assertEqual(forex, ts_item._base_obj)
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
        # Create an minimal args Listed instance
        listed = Listed(self.name, self.issuer, self.isin, self.exchange, self.ticker, self.status)
        self.session.add(listed)
        self.session.flush()
        self.listed = listed

    def test_data_frame(self):
        """Test from and to a pandas trade time-series DataFrame.

        Its or meaningful to test ths at this level as TradeEOD is rich in data
        fields and testing at parent levels would be less effective and testing
        at child levels would be more complex. So a ``Share`` test seems right.
        """
        # Create time series items from test DataFrame
        ListedEOD.from_data_frame(self.session, Listed, data_frame=self.test_df)
        # Test adding it again to test updating mechanism and idempotency
        ListedEOD.from_data_frame(self.session, Listed, data_frame=self.test_df)
        self.session.flush()
        # Get all time series items back
        df = ListedEOD.to_data_frame(self.session, Listed)
        # Test DataFrames
        pd.testing.assert_frame_equal(df.sort_index(axis=1), self.test_df.sort_index(axis=1))

    def test_dump(self):
        """Dump all class instances and their time series data to disk."""
        # Dumper
        dumper = Dump(testing=True)
        # For testing delete old any test dump folder and re-create it empty
        dumper.delete()
        dumper.makedir()
        # Create time series items from test DataFrame
        ListedEOD.from_data_frame(self.session, Listed, data_frame=self.test_df)
        # Dump methods to be tested
        ListedEOD.dump(self.session, dumper, Listed)
        # Verify dump file exists.
        self.assertTrue(dumper.exists(ListedEOD), "ListedEOD dump file not found.")
        # Clear all ListedEOD instances from the session and database
        self.session.query(ListedEOD).delete()
        # Test failing to find any ListedEOD instances when it raises SQLAlchemy
        # not found exception.
        with self.assertRaises(NoResultFound):
            self.session.query(ListedEOD).one()
        # Reuse dump to restore all ListedEOD instances
        ListedEOD.reuse(self.session, dumper, self.listed)
        self.session.flush()
        # Retrieve all ListedEOD DataFrame from the database
        dump_df = ListedEOD.to_data_frame(self.session, Listed)
        # Test DataFrames
        pd.testing.assert_frame_equal(dump_df.sort_index(axis=1), self.test_df.sort)

class TestListedEquityEOD(TestListedEOD):
    """Test a single listed equity EOD"""

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # US Domicile
        self.domicile = Domicile.factory(self.session, "US")
        self.exchange = Exchange.factory(self.session, self.mic)
        # Create an minimal args Listed instance
        listed = ListedEquity(self.name, self.issuer, self.isin, self.exchange, self.ticker, self.status)
        self.session.add(listed)
        self.session.flush()


class TestDividend(TestListedEquityEOD):
    """Test a single listed equity dividend data data."""

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
        # Dividend data fixture
        test_csv = (
            "date_stamp,currency,declaration_date,payment_date,period,record_date,unadjusted_value,adjusted_value,isin\n"
            f"2020-02-28,USD,2020-01-29,2020-03-16,Quarterly,2020-03-02,1.25,1.25,{cls.isin}\n"
            f"2020-05-29,USD,2020-05-22,2020-06-15,Quarterly,2020-06-01,1.25,1.25,{cls.isin}\n"
            f"2020-08-31,USD,2020-07-21,2020-09-15,Quarterly,2020-09-01,1.25,1.25,{cls.isin}\n"
        )
        # Convert String into StringIO
        test_df = pd.read_csv(StringIO(test_csv))
        # Convert date_stamp to pandas datetime
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        test_df['declaration_date'] = pd.to_datetime(test_df['declaration_date'])
        test_df['payment_date'] = pd.to_datetime(test_df['payment_date'])
        test_df['record_date'] = pd.to_datetime(test_df['record_date'])
        cls.test_div_df = test_df

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Get Dividend fixture
        self.ts_divs = Dividend.from_data_frame(self.session, ListedEquity, data_frame=self.test_div_df)

    def test___init__(self):
        """Initialization."""
        listed = ListedEquity.factory(self.session, isin=self.isin)
        ts_divs = self.session.query(Dividend).filter(Dividend._asset_id == listed._id).all()

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
        self.assertEqual(ts_item._base_obj, listed_equity)
        self.assertEqual(ts_item._asset_id, listed_equity._id)

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
            TestEODBase,
            TestTradeEOD,
            TestForexEOD,
            # TestIndexEOD,  # Skip for now
            # TestListedEOD,
            # TestDividend,
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
