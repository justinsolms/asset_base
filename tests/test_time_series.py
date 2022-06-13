import unittest
import datetime
import pandas as pd
from asset_base.common import TestSession

from asset_base.financial_data import SecuritiesFundamentals, SecuritiesHistory, Static

from asset_base.entity import Currency, Domicile, Issuer, Exchange
from asset_base.asset import Listed, ListedEquity
from asset_base.time_series import Dividend, TimeSeriesBase, TradeEOD
from fundmanage.utils import date_to_str


class TestTimeSeriesBase(unittest.TestCase):
    """"Common time-series ORM capability."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = TimeSeriesBase
        # Similar set up to test_financial_data
        # Securities meta-data
        cls.get_method = SecuritiesFundamentals().get_securities
        cls.securities_dataframe = cls.get_method()
        # Securities feed
        cls.feed = SecuritiesHistory()
        # Select Test security symbol identities subset
        symbols = [('AAPL', 'XNYS'), ('MCD', 'XNYS'), ('STX40', 'XJSE')]
        cls.securities_dataframe.set_index(['ticker', 'mic'], inplace=True)
        cls.securities_dataframe = cls.securities_dataframe.loc[symbols]
        cls.securities_dataframe.reset_index(drop=False, inplace=True)
        # Apple Inc.
        cls.security_item = cls.securities_dataframe[
            cls.securities_dataframe.ticker == 'AAPL']
        cls.mic = cls.security_item.mic.to_list()[0]
        cls.ticker = cls.security_item.ticker.to_list()[0]
        cls.name = cls.security_item.listed_name.to_list()[0]
        cls.issuer_domicile_code = \
            cls.security_item.issuer_domicile_code.to_list()[0]
        cls.issuer_name = cls.security_item.issuer_name.to_list()[0]
        cls.isin = cls.security_item['isin'].to_list()[0]
        cls.status = cls.security_item['status'].to_list()[0]

    def setUp(self):
        """Set up test case fixtures."""
        # Similar set up to test_financial_data
        # Each test with a clean sqlite in-memory database
        self.session = TestSession().session
        # Add all initialization objects to asset_base
        static = Static()
        Currency.update_all(self.session, get_method=static.get_currency)
        Domicile.update_all(self.session, get_method=static.get_domicile)
        Exchange.update_all(self.session, get_method=static.get_exchange)
        # Create a Listed instance, but do not commit it!! This may pollute
        # child class tests.
        self.exchange = Exchange.factory(self.session, mic=self.mic)
        self.issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)

    def test___init__(self):
        """Initialization."""
        # Get AAPL Inc instance form committed instances. Don't create this in
        # `setUp` as this risk mix-ups in the child test classes.
        self.listed = Listed(
            self.name,  self.issuer, self.isin, self.exchange, self.ticker,
            status=self.status)
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
            self.name,  self.issuer, self.isin, self.exchange, self.ticker,
            status=self.status)
        ts_item1 = TimeSeriesBase(self.listed, date_stamp=datetime.date.today())
        ts_item2 = TimeSeriesBase(self.listed, date_stamp=datetime.date.today())
        self.session.add(ts_item1)
        self.session.add(ts_item2)
        with self.assertRaises(IntegrityError):
            self.session.flush()


class TestTradeEOD(TestTimeSeriesBase):
    """A single listed security's date-stamped EOD trade data."""

    def to_dict(self, item):
        """Convert all class price attributes to a dictionary."""
        return {
            "date_stamp": item.date_stamp,
            "isin": item.listed.isin,
            "ticker": item.listed.ticker,
            "mic": item.listed.exchange.mic,
            "open": item.open,
            "close": item.close,
            "high": item.high,
            "low": item.low,
            "adjusted_close": item.adjusted_close,
            "volume": item.volume,
        }

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = TradeEOD
        # Test data
        cls.from_date = '2020-01-01'
        cls.to_date = '2020-12-31'
        cls.columns = [
            'date_stamp', 'ticker', 'mic', 'isin',
            'adjusted_close', 'close', 'high', 'low', 'open']
        cls.test_columns = [
            'adjusted_close', 'close', 'high', 'low', 'open']
        # Exclude adjusted_close as it changes
        # NOTE: These values may change as EOD historical data gets corrected
        cls.test_values = [  # Last date data
            [132.69, 134.74, 131.72, 134.08],
            [214.58, 214.93, 210.78, 211.25],
            [5460.0, 5511.0, 5403.0, 5492.0]
        ]

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Add *ALL* Listed security instances
        Listed.from_data_frame(self.session, self.securities_dataframe)
        # Securities asset_base instances list
        self.securities_list = self.session.query(Listed).all()

    def test___init__(self):
        """Initialization."""
        # Get AAPL Inc instance form committed instances. Don't create this in
        # `setUp` as this risk mix-ups in the child test classes.
        listed = Listed.factory(self.session, isin=self.isin)
        # Test for AAPL Inc.
        ts_item = TradeEOD(
            listed, date_stamp=datetime.date.today(),
            open=1.0, close=2.0, high=3.0, low=4.0,
            adjusted_close=5.0, volume=6.0)
        self.assertIsInstance(ts_item, TradeEOD)
        self.assertEqual(ts_item._asset_id, listed.id)
        self.assertEqual(ts_item.date_stamp, datetime.date.today())
        self.assertEqual(ts_item.open, 1.0)
        self.assertEqual(ts_item.close, 2.0)
        self.assertEqual(ts_item.high, 3.0)
        self.assertEqual(ts_item.low, 4.0)
        self.assertEqual(ts_item.adjusted_close, 5.0)
        self.assertEqual(ts_item.volume, 6.0)

    def test_session_commit(self):
        """Committing to the database."""
        # Get AAPL Inc instance form committed instances. Don't create this in
        # `setUp` as this risk mix-ups in the child test classes.
        listed = Listed.factory(self.session, isin=self.isin)
        # Test for AAPL Inc.
        ts_item = TradeEOD(
            listed, date_stamp=datetime.date.today(),
            open=1.0, close=2.0, high=3.0, low=4.0,
            adjusted_close=5.0, volume=6.0)
        self.session.add(ts_item)
        self.session.commit()
        self.assertEqual(ts_item.listed, listed)

    def test_from_data_frame(self):
        """Get historical EOD for a specified list of securities."""
        # This test is stolen from test_financial_data
        # Call API for data
        df = self.feed.get_eod(
            self.securities_list, self.from_date, self.to_date)
        # Keep last date for testing later
        date_stamp = df['date_stamp']
        df_last_date = date_stamp.sort_values().iloc[-1].to_pydatetime()
        # Call the tested method.
        TradeEOD.from_data_frame(self.session, Listed, data_frame=df)
        # Retrieve the submitted date stamped data from asset_base
        df = pd.DataFrame(
            [self.to_dict(item) for item in self.session.query(TradeEOD).all()])
        # Test against last date data
        last_date = datetime.datetime.strptime(self.to_date, '%Y-%m-%d').date()
        df = df[df['date_stamp'] == last_date][self.test_columns]
        # Exclude adjusted_close as it changes
        df.drop(columns='adjusted_close', inplace=True)
        self.assertFalse(df.empty)
        for i, item in enumerate(df.iterrows()):
            symbol, series = item
            self.assertEqual(series.tolist(), self.test_values[i])
        # Test security `time_series_last_date` attributes
        ts_last_date = TradeEOD.assert_last_dates(self.session, Listed)
        self.assertEqual(ts_last_date, df_last_date.date())

    def test_to_data_frame(self):
        """Convert all instances to a single data table."""
        # This test is stolen from test_financial_data
        # Call API for data
        test_df = self.feed.get_eod(
            self.securities_list, self.from_date, self.to_date)
        # Call the tested method.
        TradeEOD.from_data_frame(self.session, Listed, data_frame=test_df)
        # Method to be tested
        df = TradeEOD.to_data_frame(self.session, Listed)
        # Test - first aligning rows and columns
        df.sort_values(by=['isin', 'date_stamp'], inplace=True)
        test_df.sort_values(by=['isin', 'date_stamp'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        test_df.reset_index(drop=True, inplace=True)
        test_df = test_df[df.columns]  # Align columns rank
        pd.testing.assert_frame_equal(test_df, df)

    def test_update_all(self):
        """ Update/create all the objects in the asset_base session."""
        # This test is stolen from test_financial_data
        # Call the tested method.
        TradeEOD.update_all(self.session, Listed, self.feed.get_eod)
        # Retrieve the submitted date stamped data from asset_base
        df = pd.DataFrame(
            [self.to_dict(item) for item in self.session.query(TradeEOD).all()])
        # Test over test-date-range
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.set_index('date_stamp', inplace=True)
        df = df.loc[self.from_date:self.to_date]
        df.reset_index(inplace=True)
        # Test against last date data
        last_date = datetime.datetime.strptime(self.to_date, '%Y-%m-%d')
        df = df[df['date_stamp'] == last_date][self.test_columns]
        # Exclude adjusted_close as it changes
        df.drop(columns='adjusted_close', inplace=True)
        self.assertFalse(df.empty)
        for i, item in enumerate(df.iterrows()):
            symbol, series = item
            self.assertEqual(series.tolist(), self.test_values[i])


class TestDividend(TestTimeSeriesBase):
    """A single listed security's date-stamped EOD trade data."""

    # datetime to date-string converter
    def date_to_str(self, df):
        """Convert Timestamp objects to test date-strings."""
        # Replace pesky pandas NaT with None
        df.replace({pd.NaT: None}, inplace=True)
        for index, row in df.iterrows():
            for column, item in row.items():
                if isinstance(item, pd.Timestamp) or \
                    isinstance(item, datetime.date) or \
                        isinstance(item, datetime.datetime):
                    row[column] = item.strftime('%Y-%m-%d')
            df.loc[index] = row

    def to_dict(self, item):
        """Convert all class price attributes to a dictionary."""
        return {
            "date_stamp": item.date_stamp,
            "isin": item.listed_equity.isin,
            "ticker": item.listed_equity.ticker,
            "mic": item.listed_equity.exchange.mic,
            "currency": item.currency,
            "declaration_date": item.declaration_date,
            "payment_date": item.payment_date,
            "period": item.period,
            "record_date": item.record_date,
            "unadjusted_value": item.unadjusted_value,
            "adjusted_value": item.adjusted_value,
        }

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Dividend
        # Test data
        cls.from_date = '2020-01-01'
        cls.to_date = '2020-12-31'
        cls.columns = [
            'date_stamp', 'ticker', 'mic', 'isin',
            'currency', 'declaration_date', 'payment_date', 'period',
            'record_date', 'unadjusted_value', 'adjusted_value']
        cls.test_df = pd.DataFrame([  # Last 3 dividends
            ['2020-10-21', 'STX40', 'XJSE', 'ZAE000027108', 'ZAC', None,
                None,        None,         None, 9.1925, 9.1925],
            ['2020-11-06', 'AAPL',  'XNYS', 'US0378331005', 'USD', '2020-10-29',
                '2020-11-12', 'Quarterly', '2020-11-09', 0.2050, 0.2050],
            ['2020-11-30', 'MCD',   'XNYS', 'US5801351017', 'USD', '2020-10-08',
                '2020-12-15', 'Quarterly', '2020-12-01', 1.2900, 1.2900]],
            columns=cls.columns)

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Add *ALL* Listed security instances
        ListedEquity.from_data_frame(self.session, self.securities_dataframe)
        # Securities asset_base instances list
        self.securities_list = self.session.query(ListedEquity).all()
        # Get AAPL Inc instance form committed instances. This is different
        # from the inherited `self.listed` instance; which it must override
        # here.
        self.listed_equity = ListedEquity.factory(self.session, isin=self.isin)

    def test___init__(self):
        """Initialization."""
        # Get AAPL Inc instance form committed instances. This is different
        # from the inherited `self.listed` instance; which it must override
        # here.
        listed_equity = ListedEquity.factory(self.session, isin=self.isin)
        # Test for AAPL Inc.
        ts_item = Dividend(
            listed_equity, date_stamp=datetime.date.today(),
            currency='ZAR', declaration_date=datetime.date.today(),
            payment_date=datetime.date.today(), period='Quarterly',
            record_date=datetime.date.today(),
            unadjusted_value=1.0, adjusted_value=1.01)
        self.assertIsInstance(ts_item, Dividend)
        self.assertEqual(ts_item._asset_id, listed_equity.id)
        self.assertEqual(ts_item.date_stamp, datetime.date.today())
        self.assertEqual(ts_item.currency, 'ZAR')
        self.assertEqual(ts_item.declaration_date, datetime.date.today())
        self.assertEqual(ts_item.payment_date, datetime.date.today())
        self.assertEqual(ts_item.period, 'Quarterly')
        self.assertEqual(ts_item.record_date, datetime.date.today())
        self.assertEqual(ts_item.unadjusted_value, 1.0)
        self.assertEqual(ts_item.adjusted_value, 1.01)

    def test_session_commit(self):
        """Committing to the database."""
        # Get AAPL Inc instance form committed instances. This is different
        # from the inherited `self.listed` instance; which it must override
        # here.
        listed_equity = ListedEquity.factory(self.session, isin=self.isin)
        # Test for AAPL Inc.
        ts_item = Dividend(
            listed_equity, date_stamp=datetime.date.today(),
            currency='ZAR', declaration_date=datetime.date.today(),
            payment_date=datetime.date.today(), period='Quarterly',
            record_date=datetime.date.today(),
            unadjusted_value=1.0, adjusted_value=1.01)
        self.session.add(ts_item)
        self.session.commit()
        self.assertEqual(ts_item.listed_equity, self.listed_equity)

    def test_from_data_frame(self):
        """Get historical dividends for a specified list of securities."""
        # Test stolen from test_financial_data
        # Longer date range test causes a decision to use the EOD API service
        df = self.feed.get_dividends(
            self.securities_list, self.from_date, self.to_date)
        # Keep last date for testing later
        date_stamp = df['date_stamp']
        df_last_date = date_stamp.sort_values().iloc[-1].to_pydatetime()
        # Call the tested method.
        Dividend.from_data_frame(self.session, ListedEquity, data_frame=df)
        # Retrieve the submitted date stamped data from asset_base
        df = pd.DataFrame(
            [self.to_dict(item) for item in self.session.query(Dividend).all()])
        df.sort_values(by='date_stamp', inplace=True)
        # Test
        self.assertEqual(len(df), 12)
        df.reset_index(inplace=True, drop=True)
        self.assertEqual(set(df.columns), set(self.columns))
        # Test against last 3 dividends
        df = df.iloc[-3:].reset_index(drop=True)  # Make index 0, 1, 2
        self.date_to_str(df)  # Convert Timestamps
        self.date_to_str(self.test_df)  # Convert Timestamps
        df.replace({pd.NaT: None}, inplace=True)  # Replace pandas NaT with None
        pd.testing.assert_frame_equal(
            self.test_df.sort_index(axis='columns'),
            df.sort_index(axis='columns'),
            )
        # Test security `time_series_last_date` attributes
        ts_last_date = Dividend.assert_last_dates(self.session, ListedEquity)
        self.assertEqual(ts_last_date, df_last_date.date())

    def test_to_data_frame(self):
        """Convert all instances to a single data table."""
        # This test is stolen from test_financial_data
        # Call API for data
        test_df = self.feed.get_dividends(
            self.securities_list, self.from_date, self.to_date)
        # Call the tested method.
        Dividend.from_data_frame(self.session, ListedEquity, data_frame=test_df)
        # Method to be tested
        df = Dividend.to_data_frame(self.session, ListedEquity)
        # Test
        df.sort_values(by=['isin', 'date_stamp'], inplace=True)
        test_df.sort_values(by=['isin', 'date_stamp'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        test_df.reset_index(drop=True, inplace=True)
        test_df = test_df[df.columns]  # Align columns rank
        date_to_str(df)
        date_to_str(test_df)
        pd.testing.assert_frame_equal(test_df, df)

    def test_update_all(self):
        """Get historical dividends for a specified list of securities."""
        # Test stolen from test_financial_data
        # Call the tested method.
        Dividend.update_all(self.session, ListedEquity, self.feed.get_dividends)
        # Retrieve the submitted date stamped data from asset_base
        df = pd.DataFrame(
            [self.to_dict(item) for item in self.session.query(Dividend).all()])
        test_df = self.test_df.copy()
        # Test over test-date-range
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.set_index('date_stamp', inplace=True)
        df.sort_index(inplace=True)
        df = df.loc[self.from_date:self.to_date]
        df = df.iloc[-3:]  # Test data is for last three
        df.reset_index(inplace=True)
        df = df[test_df.columns]
        # Make dates all strings for simple testing.
        self.date_to_str(df)  # Convert Timestamps
        self.date_to_str(test_df)  # Convert Timestamps
        test_df['date_stamp'] = pd.to_datetime(test_df['date_stamp'])
        # Test
        pd.testing.assert_frame_equal(test_df, df)


class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # Classes that are passing. Add the others later when they too work.
        test_classes = [
            TestTimeSeriesBase,
            TestTradeEOD,
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


if __name__ == '__main__':

    suite = Suite()
    suite.run()
