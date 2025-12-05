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
from src.asset_base.time_series import Dividend, ForexEOD, IndexEOD, EODBase, Split
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
        import ipdb; ipdb.set_trace()
        # Test adding it again to test updating mechanism and idempotency
        ListedEOD.from_data_frame(self.session, Listed, data_frame=self.test_df)
        self.session.flush()
        # Get all time series items back
        df = ListedEOD.to_data_frame(self.session, Listed)
        # Test DataFrames
        pd.testing.assert_frame_equal(df.sort_index(axis=1), self.test_df.sort_index(axis=1))

    def test_dump_reuse(self):
        """Test dump to disk and reuse on a fresh database initialization."""
        # === PHASE 1: Create data and dump to disk ===
        # Dumper
        dumper = Dump(testing=True)
        # For testing delete old test dumps and create fresh directory
        dumper.delete()
        dumper.makedir()

        # Create time series items from test DataFrame
        ListedEOD.from_data_frame(self.session, Listed, data_frame=self.test_df)
        self.session.commit()

        # Dump the data to disk
        ListedEOD.dump(self.session, dumper, Listed)

        # Verify dump file exists
        self.assertTrue(dumper.exists("ListedEOD"), "ListedEOD dump file not found.")

        # === PHASE 2: Close current database and create fresh one ===
        # Close the current test session completely
        self.test_session.close()

        # Create a completely fresh database session (simulates fresh database initialization)
        self.test_session = TestSession()
        self.session = self.test_session.session

        # Initialize the fresh database with base entities (as would be done in real initialization)
        static = Static()
        Currency.update_all(self.session, get_method=static.get_currency)
        Domicile.update_all(self.session, get_method=static.get_domicile)
        Exchange.update_all(self.session, get_method=static.get_exchange)

        # Recreate the asset instances needed for the time series
        currency = Currency.factory(self.session, "USD")
        issuer = Issuer.factory(self.session, self.issuer_name, self.issuer_domicile_code)
        domicile = Domicile.factory(self.session, "US")
        exchange = Exchange.factory(self.session, self.mic)
        listed = Listed(self.name, issuer, self.isin, exchange, self.ticker, self.status)
        self.session.add(listed)
        self.session.commit()

        # Verify the fresh database has no ListedEOD records
        initial_count = self.session.query(ListedEOD).count()
        self.assertEqual(initial_count, 0, "Fresh database should have no ListedEOD records")

        # === PHASE 3: Reuse dump to initialize the fresh database ===
        # Reuse dump to restore all ListedEOD instances
        ListedEOD.reuse(self.session, dumper, Listed)
        self.session.commit()

        # === PHASE 4: Verify the data was restored correctly ===
        # Retrieve all ListedEOD DataFrame from the database
        dump_df = ListedEOD.to_data_frame(self.session, Listed)

        # Verify we have the expected number of records
        final_count = self.session.query(ListedEOD).count()
        expected_count = len(self.test_df)
        self.assertEqual(final_count, expected_count,
                        f"Expected {expected_count} ListedEOD records after reuse, got {final_count}")

        # Test that the DataFrames match
        pd.testing.assert_frame_equal(
            dump_df.sort_index(axis=1),
            self.test_df.sort_index(axis=1)
        )

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


class TestDividendsSplits(TestListedEquityEOD):
    """Test a single listed equity with dividend and split data."""

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
        test_df = pd.read_csv(StringIO(test_csv))
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        test_df['declaration_date'] = pd.to_datetime(test_df['declaration_date'])
        test_df['payment_date'] = pd.to_datetime(test_df['payment_date'])
        test_df['record_date'] = pd.to_datetime(test_df['record_date'])
        cls.test_div_df = test_df
        # Split data fixture
        test_csv = (
            "date_stamp,isin,numerator,denominator\n"
            f"2020-08-31,{cls.isin},2.0,1.0\n"
            f"2021-08-31,{cls.isin},4.0,1.0\n"
        )
        test_df = pd.read_csv(StringIO(test_csv))
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        cls.test_split_df = test_df

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Get Dividend fixture
        self.ts_divs = Dividend.from_data_frame(self.session, ListedEquity, data_frame=self.test_div_df)
        self.ts_splits = Split.from_data_frame(self.session, ListedEquity, data_frame=self.test_split_df)
        self.session.flush()
        # Securities list for update tests
        self.securities_list = [self.listed]

    def test___init__(self):
        """Initialization."""
        # Make sure that we can reach the correct ListedEquity, Dividends and Splits
        # from the database.
        listed = self.session.query(ListedEquity).filter(ListedEquity.isin == self.isin).one()
        ts_divs = self.session.query(Dividend).filter(Dividend._asset_id == listed._id).all()
        ts_splits = self.session.query(Split).filter(Split._asset_id == listed._id).all()
        self.assertEqual(len(ts_divs), 3)
        self.assertEqual(len(ts_splits), 2)
        # Compare with the fixture data
        divs_df = Dividend.to_data_frame(self.session, ListedEquity)
        splits_df = Split.to_data_frame(self.session, ListedEquity)
        # Convert date columns to date-strings for comparison
        self.date_to_str(divs_df)
        self.date_to_str(splits_df)
        test_divs_df = self.test_div_df.copy()
        test_splits_df = self.test_split_df.copy()
        self.date_to_str(test_divs_df)
        self.date_to_str(test_splits_df)
        # Sort rows by ticker and columns by name
        divs_df = divs_df.sort_values(['isin', 'date_stamp']).sort_index(axis='columns')
        test_divs_df = test_divs_df.sort_values(['isin', 'date_stamp']).sort_index(axis='columns')
        splits_df = splits_df.sort_values(['isin', 'date_stamp']).sort_index(axis='columns')
        test_splits_df = test_splits_df.sort_values(['isin', 'date_stamp']).sort_index(axis='columns')
        # Reset indices for test
        divs_df.reset_index(drop=True, inplace=True)
        test_divs_df.reset_index(drop=True, inplace=True)
        splits_df.reset_index(drop=True, inplace=True)
        test_splits_df.reset_index(drop=True, inplace=True)
        # Test
        pd.testing.assert_frame_equal(test_divs_df, divs_df)
        pd.testing.assert_frame_equal(test_splits_df, splits_df)


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
