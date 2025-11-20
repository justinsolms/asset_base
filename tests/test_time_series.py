from io import StringIO
import unittest
import datetime
import pandas as pd

from src.asset_base.common import TestSession
from src.asset_base.financial_data import Dump, MetaData, History, Static
from src.asset_base.entity import Currency, Domicile, Issuer, Exchange
from src.asset_base.asset import Forex, Index, Listed, ListedEquity
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
        # Similar set up to test_financial_data
        # Securities meta-data
        cls.get_method = MetaData().get_etfs
        cls.securities_dataframe = cls.get_method()
        # Securities feed
        cls.feed = History()
        # Select Test security symbol identities subset
        symbols = [("AAPL", "XNYS"), ("MCD", "XNYS"), ("STX40", "XJSE")]
        cls.securities_dataframe.set_index(["ticker", "mic"], inplace=True)
        cls.securities_dataframe = cls.securities_dataframe.loc[symbols]
        cls.securities_dataframe.reset_index(drop=False, inplace=True)
        # Apple Inc.
        cls.security_item = cls.securities_dataframe[
            cls.securities_dataframe.ticker == "AAPL"
        ]
        cls.mic = cls.security_item.mic.to_list()[0]
        cls.ticker = cls.security_item.ticker.to_list()[0]
        cls.name = cls.security_item.listed_name.to_list()[0]
        cls.issuer_domicile_code = cls.security_item.issuer_domicile_code.to_list()[0]
        cls.issuer_name = cls.security_item.issuer_name.to_list()[0]
        cls.isin = cls.security_item["isin"].to_list()[0]
        cls.status = cls.security_item["status"].to_list()[0]

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
        # Create a Listed instance, but do not commit it!! This may pollute
        # child class tests.
        self.exchange = Exchange.factory(self.session, mic=self.mic)
        self.issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code
        )

    def tearDown(self) -> None:
        """Tear down test case fixtures."""
        del self.test_session

    def test___init__(self):
        """Initialization."""
        # Get AAPL Inc instance form committed instances. Don't create this in
        # `setUp` as this risk mix-ups in the child test classes.
        self.listed = Listed(
            self.name,
            self.issuer,
            self.isin,
            self.exchange,
            self.ticker,
            status=self.status,
        )
        ts_item = TimeSeriesBase(self.listed, date_stamp=datetime.date.today())
        self.assertIsInstance(ts_item, TimeSeriesBase)
        self.assertEqual(ts_item._asset_id, self.listed.id)
        self.assertEqual(ts_item.date_stamp, datetime.date.today())

    def test_unique_constraint(self):
        """Fail the UniqueConstraint('_listed_id', 'date_stamp')"""
        from sqlalchemy.exc import IntegrityError

        # Get AAPL Inc instance form committed instances. Don't create this in
        # `setUp` as this risk mix-ups in the child test classes.
        self.listed = Listed(
            self.name,
            self.issuer,
            self.isin,
            self.exchange,
            self.ticker,
            status=self.status,
        )
        ts_item1 = TimeSeriesBase(self.listed, date_stamp=datetime.date.today())
        ts_item2 = TimeSeriesBase(self.listed, date_stamp=datetime.date.today())
        self.session.add(ts_item1)
        self.session.add(ts_item2)
        with self.assertRaises(IntegrityError):
            self.session.flush()


class TestSimpleEOD(TestTimeSeriesBase):
    """A simple time series with a price."""

    def test___init__(self):
        """Test class polymorphism functionality."""
        # Create a Cash instance and add it to the session
        listed = Listed(
            self.name,
            self.issuer,
            self.isin,
            self.exchange,
            self.ticker,
            status=self.status,
        )
        ts_item = SimpleEOD(listed, date_stamp=datetime.date.today(), close=123.45)
        self.session.add(ts_item)
        # Query the superclass Asset which should produce a Cash polymorphic
        # instance
        instance = self.session.query(TimeSeriesBase).one()
        self.assertEqual(instance.class_name, "SimpleEOD")
        self.assertEqual(instance._discriminator, "simple_eod")
        self.assertEqual(ts_item, instance)


class TestTradeEOD(TestTimeSeriesBase):

    """A single listed security's date-stamped EOD trade data."""

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
        cls.Cls = TradeEOD
        # Test data
        cls.from_date = datetime.datetime.strptime("2020-01-01", "%Y-%m-%d").date()
        cls.to_date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d").date()
        cls.columns = [
            "date_stamp",
            "ticker",
            "mic",
            "isin",
            "adjusted_close",
            "close",
            "high",
            "low",
            "open",
        ]
        cls.test_columns = ["adjusted_close", "close", "high", "low", "open"]
        # Exclude adjusted_close as it changes

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()


class TestForexEOD(TestTradeEOD):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Forex
        # Lessen the number of forex tickers for shorter testing.
        # NOTE: Remember to use `self.Cls` instead of `Forex` for less tickers.
        cls.Cls.foreign_currencies = ["USD", "EUR", "ZAR"]
        # Currency data
        cls.get_method = History().get_forex
        # Test strings
        cls.name = "USDZAR"
        cls.test_str = "One USD priced in ZAR"
        cls.key_code = "USDZAR"
        cls.identity_code = "USDZAR"
        # Test values
        cls.columns = [
            "date_stamp",
            "ticker",
            "close",
            "high",
            "low",
            "open",
            "adjusted_close",
            "volume",
        ]
        cls.test_columns = ["close", "high", "low", "open", "volume"]
        # Exclude adjusted_close as it varies
        # NOTE: These values may change as EOD historical data gets corrected
        cls.test_values = pd.DataFrame(
            [  # Last date data
                [14.6878, 14.7204, 14.5707, 14.6078, 0],
                [1.0000, 1.0000, 1.0000, 1.0000, 0],
                [0.8185, 0.8191, 0.8123, 0.8131, 89060],
            ],
            columns=cls.test_columns,
        )

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Currencies for Forex tests
        self.base_currency = Currency.factory(self.session, Forex.root_currency_ticker)
        self.price_currency = Currency.factory(
            self.session, Forex.foreign_currencies[-1]
        )  # Pick last one
        # Tickers
        self.base_ticker = self.base_currency.ticker
        self.price_ticker = self.price_currency.ticker
        # Add *ALL* Listed security instances
        Listed.from_data_frame(self.session, self.securities_dataframe)

    def test___init__(self):
        """Initialization."""
        # Get AAPL Inc instance form committed instances. Don't create this in
        # `setUp` as this risk mix-ups in the child test classes.
        forex = Forex.factory(self.session, self.base_ticker, self.price_ticker)
        # Test for AAPL Inc.
        forex_eod = ForexEOD(
            forex,
            date_stamp=datetime.date.today(),
            open=1.0,
            close=2.0,
            high=3.0,
            low=4.0,
            adjusted_close=5.0,
            volume=6.0,
        )
        self.session.add(forex_eod)
        self.assertIsInstance(forex_eod, ForexEOD)
        # test the inherited Asset backref
        self.assertEqual(forex_eod.base_obj, forex)
        self.assertEqual(forex_eod.date_stamp, datetime.date.today())
        self.assertEqual(forex_eod._close, forex_eod._close)
        self.assertEqual(forex_eod._open, 1.0)
        self.assertEqual(forex_eod._close, 2.0)
        self.assertEqual(forex_eod._high, 3.0)
        self.assertEqual(forex_eod._low, 4.0)
        self.assertEqual(forex_eod._adjusted_close, 5.0)
        self.assertEqual(forex_eod._volume, 6.0)

        # Test polymorphism functionality by query of the superclass
        # TimeSeriesBase which should produce a ListedEOD polymorphic instance
        instance = self.session.query(TimeSeriesBase).one()
        self.assertEqual(instance.class_name, "ForexEOD")
        self.assertEqual(instance._discriminator, "forex_eod")
        self.assertEqual(forex_eod, instance)

        # Test Forex._eod_series <-> ForexEOD.forex relationship
        self.assertEqual(forex._series[0], forex_eod)
        self.assertEqual(forex._eod_series[0], forex_eod)
        self.assertEqual(forex_eod.forex, forex)

        # Test that ListedEquity._eod_series lists purely ListedEOD instances
        # and Forex._eod_series lists purely ForexEOD instances.
        # This requires the addition of a Listed in addition to the ListedEOD.
        self.assertEqual(len(forex._series), 1)
        listed = Listed.factory(self.session, isin=self.isin)
        listed_eod = ListedEOD(
            listed,
            date_stamp=datetime.date.today(),
            open=1.0,
            close=2.0,
            high=3.0,
            low=4.0,
            adjusted_close=5.0,
            volume=6.0,
        )
        self.session.add(listed_eod)
        # (again) Test Forex._eod_series <-> ForexEOD.forex relationship
        self.assertEqual(forex._series[0], forex_eod)
        self.assertEqual(forex._eod_series[0], forex_eod)
        self.assertEqual(forex_eod.forex, forex)
        # Test Listed._eod_series <-> ListedEOD.listed relationship
        self.assertEqual(len(listed._eod_series), 1)
        self.assertEqual(listed._eod_series[0], listed_eod)
        self.assertEqual(listed_eod.listed, listed)
        # There should now be two time TimeSeriesBase instances
        self.assertEqual(len(forex._series), 1)
        self.assertEqual(len(listed._series), 1)
        # Test equivalence of the TimeSeriesBase._series parent attribute
        self.assertEqual(forex._series, forex._eod_series)
        self.assertEqual(listed._series, listed._eod_series)


class TestIndexEOD(TestTradeEOD):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Index
        # Index data getter
        cls.get_method = History().get_indices
        # Test strings
        cls.index_list = ("GSPC", "ASX", "J200")
        cls.name = "The Index"
        cls.currency_code = "ZAR"
        cls.test_str = "The Index priced in ZAR"
        cls.key_code = "XYZ.INDX"
        cls.identity_code = "XYZ.INDX"
        # Test values
        cls.test_values = pd.DataFrame(
            [  # Last date data
                [3673.63, 3723.98, 3664.69, 3723.98, 49334000.0],
                [3756.0701, 3760.2, 3726.8799, 3733.27, 3172510000.0],
                [54379.58, 54615.33, 53932.88, 54615.33, 0.0],
            ],
            columns=cls.test_columns,
        )

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Currencies for Forex tests
        self.currency = Currency.factory(self.session, self.currency_code)
        # Add *ALL* Listed security instances
        Listed.from_data_frame(self.session, self.securities_dataframe)

    def test___init__(self):
        """Initialization."""
        # Get AAPL Inc instance form committed instances. Don't create this in
        # `setUp` as this risk mix-ups in the child test classes.
        index = Index.factory(self.session, self.name, self.ticker, self.currency_code)
        # Test for AAPL Inc.
        index_eod = IndexEOD(
            index,
            date_stamp=datetime.date.today(),
            open=1.0,
            close=2.0,
            high=3.0,
            low=4.0,
            adjusted_close=5.0,
            volume=6.0,
        )
        self.session.add(index_eod)
        self.assertIsInstance(index_eod, IndexEOD)
        # test the inherited Asset backref
        self.assertEqual(index_eod.base_obj, index)
        self.assertEqual(index_eod.date_stamp, datetime.date.today())
        self.assertEqual(index_eod._close, index_eod._close)
        self.assertEqual(index_eod._open, 1.0)
        self.assertEqual(index_eod._close, 2.0)
        self.assertEqual(index_eod._high, 3.0)
        self.assertEqual(index_eod._low, 4.0)
        self.assertEqual(index_eod._adjusted_close, 5.0)
        self.assertEqual(index_eod._volume, 6.0)

        # Test polymorphism functionality by query of the superclass
        # TimeSeriesBase which should produce a ListedEOD polymorphic instance
        instance = self.session.query(TimeSeriesBase).one()
        self.assertEqual(instance.class_name, "IndexEOD")
        self.assertEqual(instance._discriminator, "index_eod")
        self.assertEqual(index_eod, instance)

        # Test Forex._eod_series <-> IndexEOD.index relationship
        self.assertEqual(index._series[0], index_eod)
        self.assertEqual(index._eod_series[0], index_eod)
        self.assertEqual(index_eod.index, index)

        # Test that `ListedEquity._eod_series` lists purely `ListedEOD`
        # instances and Forex._eod_series lists purely IndexEOD instances. This
        # requires the addition of a `Listed` in addition to the `ListedEOD`.
        self.assertEqual(len(index._series), 1)
        listed = Listed.factory(self.session, isin=self.isin)
        listed_eod = ListedEOD(
            listed,
            date_stamp=datetime.date.today(),
            open=1.0,
            close=2.0,
            high=3.0,
            low=4.0,
            adjusted_close=5.0,
            volume=6.0,
        )
        self.session.add(listed_eod)
        # (again) Test Forex._eod_series <-> IndexEOD.index relationship
        self.assertEqual(index._series[0], index_eod)
        self.assertEqual(index._eod_series[0], index_eod)
        self.assertEqual(index_eod.index, index)
        # Test Listed._eod_series <-> ListedEOD.listed relationship
        self.assertEqual(len(listed._eod_series), 1)
        self.assertEqual(listed._eod_series[0], listed_eod)
        self.assertEqual(listed_eod.listed, listed)
        # There should now be two time TimeSeriesBase instances
        self.assertEqual(len(index._series), 1)
        self.assertEqual(len(listed._series), 1)
        # Test equivalence of the TimeSeriesBase._series parent attribute
        self.assertEqual(index._series, index._eod_series)
        self.assertEqual(listed._series, listed._eod_series)


class TestListedEOD(TestTradeEOD):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = ListedEOD

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Add *ALL* Listed security instances
        Listed.from_data_frame(self.session, self.securities_dataframe)
        # Securities asset_base instances list
        self.securities_list = self.session.query(Listed).all()
        # Test data
        self.date = datetime.datetime.strptime("2020-12-31", "%Y-%m-%d").date()
        # Test data
        test_csv = (
            "date_stamp,adjusted_close,close,high,low,open,volume,isin\n"
            "2020-12-31,130.3872,132.69,134.74,131.72,134.08,99116600,US0378331005\n"
            "2020-12-31,201.8414,214.58,214.93,210.78,211.25,2610900,US5801351017\n"
            "2020-12-31,51.851674,54.600,55.110,54.030,54.920,112700,ZAE000027108\n"
        )
        test_io = StringIO(test_csv)   # Convert String into StringIO
        test_df = pd.read_csv(test_io)
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        self.test_df = test_df

    def test___init__(self):
        """Initialization."""
        # Get AAPL Inc instance form committed instances. Don't create this in
        # `setUp` as this risk mix-ups in the child test classes.
        listed = Listed.factory(self.session, isin=self.isin)
        # Test for AAPL Inc.
        listed_eod = ListedEOD(
            listed,
            date_stamp=datetime.date.today(),
            open=1.0,
            close=2.0,
            high=3.0,
            low=4.0,
            adjusted_close=5.0,
            volume=6.0,
        )
        self.session.add(listed_eod)
        self.assertIsInstance(listed_eod, ListedEOD)
        # test the inherited Asset backref
        self.assertEqual(listed_eod.base_obj, listed)
        self.assertEqual(listed_eod.date_stamp, datetime.date.today())
        self.assertEqual(listed_eod._close, listed_eod._close)
        self.assertEqual(listed_eod._open, 1.0)
        self.assertEqual(listed_eod._close, 2.0)
        self.assertEqual(listed_eod._high, 3.0)
        self.assertEqual(listed_eod._low, 4.0)
        self.assertEqual(listed_eod._adjusted_close, 5.0)
        self.assertEqual(listed_eod._volume, 6.0)

        # Test polymorphism functionality by query of the superclass
        # TimeSeriesBase which should produce a ListedEOD polymorphic instance
        instance = self.session.query(TimeSeriesBase).one()
        self.assertEqual(instance.class_name, "ListedEOD")
        self.assertEqual(instance._discriminator, "listed_eod")
        self.assertEqual(listed_eod, instance)

        # Test Listed._eod_series <-> ListedEOD.listed relationship
        self.assertEqual(listed._series[0], listed_eod)
        self.assertEqual(listed._eod_series[0], listed_eod)
        self.assertEqual(listed_eod.listed, listed)

    def test_session_commit(self):
        """Committing to the database."""
        # Get AAPL Inc instance form committed instances. Don't create this in
        # `setUp` as this risk mix-ups in the child test classes.
        listed = Listed.factory(self.session, isin=self.isin)
        # Test for AAPL Inc.
        ts_item = ListedEOD(
            listed,
            date_stamp=datetime.date.today(),
            open=1.0,
            close=2.0,
            high=3.0,
            low=4.0,
            adjusted_close=5.0,
            volume=6.0,
        )
        self.session.add(ts_item)
        self.session.commit()
        self.assertEqual(ts_item.base_obj, listed)

    def test_data_frame(self):
        """To and from dataframe."""
        test_df = self.test_df
        # Methods tested
        df = self.feed.get_eod(self.securities_list, self.date, self.date)
        ListedEOD.from_data_frame(self.session, Listed, data_frame=df)
        df = ListedEOD.to_data_frame(self.session, Listed)
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

    def test_update_all(self):
        """Update/create all the objects in the asset_base session."""
        test_df = self.test_df
        # Methods tested
        ListedEOD.update_all(self.session, self.feed.get_eod)
        df = ListedEOD.to_data_frame(self.session, Listed)
        # Test date range
        df = df[df.date_stamp.dt.date == self.date]
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
