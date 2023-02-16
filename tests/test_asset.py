import unittest
import datetime
import pandas as pd
import pandas

from asset_base.financial_data import Dump, MetaData
from asset_base.financial_data import History, Static

from asset_base.common import TestSession
from asset_base.exceptions import FactoryError, BadISIN, ReconcileError
from asset_base.entity import Currency, Domicile, Issuer, Exchange
from asset_base.asset import Asset, Base, Cash, Forex, Index, Listed, ListedEquity, Share
from asset_base.time_series import Dividend, ForexEOD, IndexEOD, ListedEOD
from fundmanage3.utils import date_to_str


class TestBase(unittest.TestCase):
    """Test Base class and provide a base class for child tests."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Base
        # Fixed date window for time series tests
        cls.from_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d').date()
        cls.to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d').date()
        # Currency data
        cls.get_method = Static().get_currency
        cls.currency_dataframe = cls.get_method()
        # A single domicile with currency
        cls.currency_item = cls.currency_dataframe[
            cls.currency_dataframe.ticker == 'USD']
        cls.currency_name = cls.currency_item.name.to_list()[0]
        cls.currency_ticker = cls.currency_item.ticker.to_list()[0]

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean sqlite in-memory database
        self.session = TestSession().session
        # Add all Currency objects to asset_base
        Currency.update_all(self.session, get_method=Static().get_currency)
        self.currency = Currency.factory(self.session, self.currency_ticker)


class TestAsset(TestBase):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Asset
        # Test strings
        cls.name = 'Test Asset'
        cls.test_str = 'Test Asset is an Asset priced in USD.'
        cls.key_code = 'USD.Test Asset'
        cls.identity_code = 'USD.Test Asset'

    def test___init__(self):
        asset = Asset(self.name, self.currency)
        self.assertIsInstance(asset, Asset)

    def test___str__(self):
        asset = Asset(self.name, self.currency)
        self.assertEqual(self.test_str, asset.__str__())

    def test_key_code(self):
        asset = Asset(self.name, self.currency)
        self.assertEqual(self.key_code, asset.key_code)

    def test_identity_code(self):
        asset = Asset(self.name, self.currency)
        self.assertEqual(self.identity_code, asset.identity_code)

    def test_factory(self):
        """Test session add asset but domicile and currency already added."""
        # FIXME: Drop test. We needed it only for a bug we had.
        # Pre-add currency.
        # Add.
        asset = Asset.factory(self.session, self.name, self.currency_ticker)
        asset = Asset.factory(self.session, self.name, self.currency_ticker)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Asset).all()), 1)
        # Attributes
        self.assertEqual(asset.name, self.name)
        self.assertEqual(asset.currency_ticker, self.currency.ticker)
        # Get same
        asset1 = Asset.factory(self.session, self.name, self.currency_ticker)
        self.assertEqual(asset, asset1)


class TestCash(TestAsset):
    # TODO: Re-code for same tests cases as Entity

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Cash
        # The convention used by this module is to use yesterday's close price
        # due to the limitation imposed by the database price feed.
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        # Round off the date to remove the time and keep only the date
        # component.
        date_stamp = datetime.date(
            yesterday.year, yesterday.month, yesterday.day)
        cls.test_price_dict = {'date_stamp': date_stamp, "close": 1.0}
        # Test strings
        cls.test_str = 'U.S. Dollar is an Cash priced in USD.'
        cls.key_code = 'USD'
        cls.identity_code = 'USD'

    def test___init__(self):
        # Produce the Cash item with the currency
        cash = Cash(self.currency)
        self.assertIsInstance(cash, Cash)
        self.assertEqual(cash.currency, self.currency)
        self.assertEqual(cash.name, self.currency_name)
        self.assertEqual(cash.ticker, self.currency_ticker)

        # Test class the polymorphism functionality.
        # Query the superclass Asset which should produce a Cash polymorphic
        # instance
        self.session.add(cash)
        instance = self.session.query(Asset).one()  # There are two, see above!
        self.assertEqual(instance._class_name, 'Cash')
        self.assertEqual(instance._discriminator, 'cash')

    def test___str__(self):
        currency = Currency.factory(self.session, self.currency_ticker)
        cash = Cash(currency)
        self.assertEqual(self.test_str, cash.__str__())

    def test_key_code(self):
        currency = Currency.factory(self.session, self.currency_ticker)
        cash = Cash(currency)
        self.assertEqual(self.key_code, cash.key_code)

    def test_identity_code(self):
        currency = Currency.factory(self.session, self.currency_ticker)
        cash = Cash(currency)
        self.assertEqual(self.identity_code, cash.ticker)

    def test_get_locality(self):
        currency = Currency.factory(self.session, self.currency_ticker)
        cash = Cash(currency)
        self.assertTrue(cash.get_locality('US') == 'domestic')
        self.assertTrue(cash.get_locality('UK') == 'foreign')

    def test_factory(self):
        """Test session add entity with domicile and currency already added."""
        # Create new Cash instance
        Cash.factory(self.session, self.currency_ticker)
        # Retrieve it
        cash = Cash.factory(self.session, self.currency_ticker)
        # Test
        self.assertIsInstance(cash, Cash)
        self.assertEqual(cash.name, self.currency_name)
        self.assertEqual(cash.ticker, self.currency_ticker)

    def test_factory_fail(self):
        """Test session add fail if second add has wrong currency ticker."""
        wrong_ticker = '---'
        with self.assertRaises(FactoryError):
            Cash.factory(self.session, wrong_ticker)

    def test_update_all(self):
        """Create all ``Cash`` instances form all ``Currency`` instances."""
        Cash.update_all(self.session)
        # Test data
        currencies = self.session.query(Currency).all()
        test_codes = list(set(item.ticker for item in currencies))
        test_codes.sort()
        # Test Cash
        cash_list = self.session.query(Cash).all()
        codes = [item.ticker for item in cash_list]
        codes.sort()
        # Assert
        self.assertEqual(codes, test_codes)

    def test_time_series(self):
        """Retrieve historic time-series for a set of class instances."""
        currency = Currency.factory(self.session, self.currency_ticker)
        cash = Cash(currency)
        # Simulate instrument price data date index
        date_index = pd.date_range(start='2000/01/01', periods=10)
        # Test the method
        time_series = cash.time_series(date_index=date_index)
        # Test
        self.assertTrue((time_series.index == date_index).all())
        self.assertTrue((time_series == 1.0).all())
        # Check Cash name
        self.assertEqual(time_series.name.identity_code, cash.identity_code)


class TestForex(TestAsset):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Forex
        # Lessen the number of forex tickers for shorter testing.
        # NOTE: Remember to use `self.Cls` instead of `Forex` for less tickers.
        cls.Cls.foreign_currencies = ['USD', 'EUR', 'ZAR']
        # Currency data
        cls.get_method = History().get_forex
        # Test strings
        cls.name = 'USDZAR'
        cls.test_str = 'One USD priced in ZAR'
        cls.key_code = 'USDZAR'
        cls.identity_code = 'USDZAR'
        # Test values
        cls.columns = [
            'date_stamp', 'ticker', 'close', 'high', 'low', 'open',
            'adjusted_close', 'volume']
        cls.test_columns = ['close', 'high', 'low', 'open', 'volume']
        # Exclude adjusted_close as it varies
        # NOTE: These values may change as EOD historical data gets corrected
        cls.test_values = pd.DataFrame([  # Last date data
            [14.6878, 14.7204, 14.5707,  14.6078,    0],
            [1.0000,   1.0000,  1.0000,   1.0000,    0],
            [0.8185,   0.8191,  0.8123,   0.8131, 89060],
        ], columns=cls.test_columns)

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Currencies for Forex tests
        self.base_currency = Currency.factory(
            self.session, Forex.root_currency_ticker)
        self.price_currency = Currency.factory(
            self.session, Forex.foreign_currencies[-1])  # Pick last one
        # Tickers
        self.base_ticker = self.base_currency.ticker
        self.price_ticker = self.price_currency.ticker

    def test___init__(self):
        forex = Forex(self.base_currency, self.price_currency)
        self.assertIsInstance(forex, Forex)
        self.assertEqual(self.name, forex.name)
        self.assertEqual(self.identity_code, forex.identity_code)

    def test___str__(self):
        forex = Forex(self.base_currency, self.price_currency)
        self.assertEqual(self.test_str, forex.__str__())

    def test_key_code(self):
        forex = Forex(self.base_currency, self.price_currency)
        self.assertEqual(self.key_code, forex.key_code)

    def test_identity_code(self):
        forex = Forex(self.base_currency, self.price_currency)
        self.assertEqual(self.identity_code, forex.identity_code)

    def test_factory(self):
        forex = Forex.factory(
            self.session, self.base_ticker, self.price_ticker)
        forex = Forex.factory(
            self.session, self.base_ticker, self.price_ticker)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Asset).all()), 1)
        # Verify strings
        self.assertEqual(self.name, forex.name)
        self.assertEqual(self.test_str, forex.__str__())
        self.assertEqual(self.key_code, forex.key_code)
        self.assertEqual(self.identity_code, forex.identity_code)

    def test_update_all(self):
        """Full test forex time series history."""
        self.Cls.update_all(self.session, self.get_method)
        # Make sure they are all present
        forex_list = self.session.query(Forex).all()
        ticker_list = [forex.price_currency_ticker for forex in forex_list]
        self.assertEqual(set(Forex.foreign_currencies), set(ticker_list))
        # Retrieve the submitted ListedEOD data from asset_base
        df = pd.DataFrame([item.to_dict()
                           for item in self.session.query(ForexEOD).all()])
        # Test
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.sort_values(['date_stamp'], inplace=True)
        # Test against last date test_values data
        last_date = pd.to_datetime(self.to_date)
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        # Sort to remove ambiguity
        df.sort_values(by='close', inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.test_values.sort_values(by='close', inplace=True)
        self.test_values.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)

    def test_get_eod(self):
        """Return the EOD time series for the asset."""
        self.Cls.update_all(self.session, self.get_method)
        # Test AAPL Inc.
        forex = self.Cls.factory(
            self.session, self.base_ticker, self.Cls.foreign_currencies[0])
        forex1 = self.Cls.factory(
            self.session, self.base_ticker, self.Cls.foreign_currencies[1])
        forex2 = self.Cls.factory(
            self.session, self.base_ticker, self.Cls.foreign_currencies[2])
        # Method to be tested
        df = forex.get_eod()
        df1 = forex1.get_eod()
        df2 = forex2.get_eod()
        # Make to-test data
        df.reset_index(inplace=True)
        df1.reset_index(inplace=True)
        df2.reset_index(inplace=True)
        df['ticker'] = forex.ticker
        df1['ticker'] = forex1.ticker
        df2['ticker'] = forex2.ticker
        df = pd.concat([df, df1, df2], axis='index')
        # Test against last date test_values data
        last_date = pd.to_datetime(self.to_date)
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        df.reset_index(drop=True, inplace=True)
        # Sort to remove ambiguity
        df.sort_values(by='close', inplace=True)
        self.test_values.sort_values(by='close', inplace=True)
        self.test_values.reset_index(drop=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)

    def test_get_rates_data_frame(self):
        """Price the base in a list of pricing currencies."""
        # Test data
        date = pd.to_datetime(self.to_date)
        test_values = [0.06808371573687005, 0.05593077247783875, 1.0]
        # Populate
        self.Cls.update_all(self.session, self.get_method)
        # Test
        df = self.Cls.get_rates_data_frame(
            self.session, 'ZAR', self.Cls.foreign_currencies)
        self.assertTrue(all(df['ZAR'] == 1.0))
        self.assertEqual(df.loc[date].tolist(), test_values)


class TestShare(TestAsset):
    """Tests to be implemented by child classes

    Note
    ----
    The Asset class is not supposed to be directly instantiated.
    """

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Share
        # Set up Domicile data
        cls.get_method = Static().get_domicile
        cls.domicile_dataframe = cls.get_method()
        # A single domicile with currency
        cls.domicile_item = cls.domicile_dataframe[
            cls.domicile_dataframe.country_code == 'US']
        cls.domicile_code = cls.domicile_item.country_code.to_list()[0]
        cls.domicile_name = cls.domicile_item.country_name.to_list()[0]
        cls.domicile_currency = cls.domicile_item.currency_ticker.to_list()[0]
        # Issuer test strings
        cls.issuer_name = 'The Issuer'
        cls.issuer_domicile_code = 'US'
        # Share test strings
        cls.name = 'Test Share'
        cls.key_code = 'US.The Issuer.Test Share'
        cls.identity_code = 'US.The Issuer.Test Share'
        cls.quote_units = 'cents'

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Add all Domicile objects to the asset_base
        Domicile.update_all(self.session, get_method=Static().get_domicile)
        self.domicile = Domicile.factory(self.session, self.domicile_code)
        # Issuer
        self.issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)

    def test___init__(self):
        share = Share(
            self.name, self.issuer, quote_units=self.quote_units)
        self.assertIsInstance(share, Share)
        self.assertEqual(share.name, self.name)
        self.assertEqual(share.issuer, self.issuer)
        self.assertEqual(share.currency, self.issuer.currency)
        self.assertEqual(share.quote_units, self.quote_units)

    def test___str__(self):
        share = Share(self.name, self.issuer)
        self.assertEqual(
            share.__str__(),
            'Test Share is a Share issued by The Issuer in United States.')

    def test_key_code(self):
        share = Share(self.name, self.issuer)
        self.assertEqual(share.key_code, self.key_code)

    def test_identity_code(self):
        share = Share(self.name, self.issuer)
        self.assertEqual(share.identity_code, self.identity_code)


class TestListed(TestShare):
    """Any kind of listed financial share."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Listed
        # Securities meta-data
        cls.get_meta_method = MetaData().get_etfs
        cls.securities_dataframe = cls.get_meta_method()
        # Securities EOD-data
        cls.get_eod_method = History().get_eod
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
        cls.test_str = (
            'Apple Inc (AAPL.XNYS) ISIN:US0378331005 is a listed '
            'on the USA Stocks issued by Apple Inc in United States')
        # about where it belongs. MacDonald Inc.
        cls.security_item1 = cls.securities_dataframe[
            cls.securities_dataframe.ticker == 'MCD']
        cls.mic1 = cls.security_item1.mic.to_list()[0]
        cls.ticker1 = cls.security_item1.ticker.to_list()[0]
        cls.name1 = cls.security_item1.listed_name.to_list()[0]
        cls.issuer_domicile_code1 = \
            cls.security_item1.issuer_domicile_code.to_list()[0]
        cls.issuer_name1 = cls.security_item1.issuer_name.to_list()[0]
        cls.isin1 = cls.security_item1['isin'].to_list()[0]
        cls.status1 = cls.security_item1['status'].to_list()[0]
        # MacDonald Inc.
        cls.security_item2 = \
            cls.securities_dataframe[cls.securities_dataframe.ticker == 'STX40']
        cls.mic2 = cls.security_item2.mic.to_list()[0]
        cls.ticker2 = cls.security_item2.ticker.to_list()[0]
        cls.name2 = cls.security_item2.listed_name.to_list()[0]
        cls.issuer_domicile_code2 = \
            cls.security_item2.issuer_domicile_code.to_list()[0]
        cls.issuer_name2 = cls.security_item2.issuer_name.to_list()[0]
        cls.isin2 = cls.security_item2['isin'].to_list()[0]
        cls.status2 = cls.security_item2['status'].to_list()[0]
        # Selected securities meta-data only including above 3 securities
        isins = [cls.isin, cls.isin1, cls.isin2]
        data_frame = cls.securities_dataframe
        cls.selected_securities_dataframe = data_frame[data_frame['isin'].isin(
            isins)]
        # NOTE: The `from_date` and `to_date` are inherited from  TestAsset
        cls.columns = [
            'date_stamp', 'ticker', 'mic', 'isin',
            'close', 'high', 'low', 'open', 'volume']
        cls.test_columns = [
            'close', 'high', 'low', 'open', 'volume']
        # Exclude adjusted_close as it varies
        # NOTE: These values may change as EOD historical data gets corrected
        cls.test_values = pd.DataFrame([  # Last date data
            [132.69, 134.74, 131.72, 134.08, 99116586.0],
            [214.58, 214.93, 210.78, 211.25, 2610900.0],
            [5460.0, 5511.0, 5403.0, 5492.0, 112700.0]
        ], columns=cls.test_columns)

        # Do not create Issuer instance here as it should be created during
        # testing only as later the Listed.factory method will create the issuer
        # from it's arguments.

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Add all Exchange objects to asset_base
        Exchange.update_all(self.session, get_method=Static().get_exchange)
        self.exchange = Exchange.factory(self.session, mic=self.mic)
        self.exchange1 = Exchange.factory(self.session, mic=self.mic1)
        self.exchange2 = Exchange.factory(self.session, mic=self.mic2)

    def to_eod_dict(self, item):
        """Convert all class price attributes to a dictionary."""
        return {
            "date_stamp": item.date_stamp,
            "isin": item.base_obj.isin,
            "ticker": item.base_obj.ticker,
            "mic": item.base_obj.exchange.mic,
            "open": item.open,
            "close": item.close,
            "high": item.high,
            "low": item.low,
            "adjusted_close": item.adjusted_close,
            "volume": item.volume,
        }

    def test___init__(self):
        """Initialization."""
        # Create Issuer instance here as it should be created during testing
        # only as later on the Listed.factory method will be tested to create
        # the issuer from it's arguments.
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        listed = Listed(
            self.name,  issuer, self.isin, self.exchange, self.ticker,
            status=self.status)
        self.assertIsInstance(listed, Listed)
        self.assertEqual(listed.name, self.name)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)

    def test___str__(self):
        """Full parameter set provided."""
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        listed = Listed(
            self.name,  issuer, self.isin, self.exchange, self.ticker)
        self.assertEqual(listed.__str__(), self.test_str)

    def test_key_code(self):
        """Full parameter set provided."""
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        listed = Listed(
            self.name,  issuer, self.isin, self.exchange, self.ticker)
        self.assertEqual(listed.key_code, self.isin)

    def test_identity_code(self):
        """Full parameter set provided."""
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        listed = Listed(
            self.name,  issuer, self.isin, self.exchange, self.ticker)
        self.assertEqual(
            listed.identity_code, f'{self.isin}.{self.ticker}')

    def test_get_locality(self):
        """Test the domestic or foreign status of a share."""
        # Add. Issuer should be automatically created.
        listed = Listed.factory(
            self.session, self.isin, self.mic, self.ticker, self.name,
            self.issuer_domicile_code, self.issuer_name)
        #  Test.
        self.assertEqual(listed.get_locality('US'), 'domestic')
        self.assertEqual(listed.get_locality('GB'), 'foreign')

    def test_factory(self):
        """Full suite of factory parameters with previously existing issuer."""
        # Add. Issuer should be automatically created.
        listed = Listed.factory(
            self.session, self.isin, self.mic, self.ticker, self.name,
            self.issuer_domicile_code, self.issuer_name)
        # Inspect database for expected number of entities
        self.assertEqual(len(self.session.query(Issuer).all()), 1)
        self.assertEqual(len(self.session.query(Listed).all()), 1)
        # Different query argument sets produce the same instance
        # Firstly buy ISIN
        listed1 = Listed.factory(self.session, isin=self.isin)
        self.assertEqual(listed, listed1)
        # Secondly by (MIC,ticker)
        listed2 = Listed.factory(self.session, mic=self.mic, ticker=self.ticker)
        self.assertEqual(listed, listed2)
        # Attributes
        self.assertEqual(listed.name, self.name)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN country code, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)

    def test_factory_change(self):
        """Changes to existing instances."""
        # Add. Issuer should be automatically created.
        listed = Listed.factory(
            self.session, self.isin, self.mic, self.ticker, self.name,
            self.issuer_domicile_code, self.issuer_name,)
        # Changes name
        listed1 = Listed.factory(
            self.session, self.isin, listed_name='new_name')
        self.assertEqual(listed1, listed)
        self.assertEqual(listed1.name, 'new_name')
        # Change Exchange
        listed4 = Listed.factory(self.session, self.isin, mic='XLON')
        self.assertEqual(listed4.name, 'new_name')
        # Change ticker
        listed5 = Listed.factory(self.session, self.isin, ticker='ABC')
        self.assertEqual(listed5.ticker, 'ABC')
        # Change status
        listed6 = Listed.factory(self.session, self.isin, status='delisted')
        self.assertEqual(listed6.status, 'listed')

    def test_factory_fail_change(self):
        """Fail with issuer change attempt."""
        # Add. Issuer should be automatically created.
        Listed.factory(
            self.session, self.isin, self.mic, self.ticker, self.name,
            self.issuer_domicile_code, self.issuer_name)
        # New issuer
        with self.assertRaises(ReconcileError):
            Listed.factory(self.session, self.isin, issuer_domicile_code='GB')
        # New issuer
        with self.assertRaises(ReconcileError):
            Listed.factory(
                self.session, self.isin, issuer_name='new_issuer_name')
        # Check there are no new issuers as a result of the above
        self.assertEqual(len(self.session.query(Issuer).all()), 1)

    def test_factory_fail_wrong_args(self):
        """Fail with incorrect arguments."""
        # Add. Issuer should be automatically created.
        Listed.factory(
            self.session,
            isin=self.isin, mic=self.mic, ticker=self.ticker,
            listed_name=self.name,
            issuer_domicile_code=self.issuer_domicile_code,
            issuer_name=self.issuer_name)
        # Test retrieval on ISIN fails Issuer requirement
        with self.assertRaises(FactoryError):
            Listed.factory(
                self.session,
                self.isin1)  # Wrong ISIN
        # Test retrieval on MIC, Ticker pair fails Issuer requirement
        with self.assertRaises(FactoryError):
            Listed.factory(
                self.session,
                ticker=self.ticker1,  # Wrong ticker
                mic=self.mic)
        # Test retrieval on MIC, Ticker pair fails Listed.__init__ argument
        # requirements
        with self.assertRaises(FactoryError):
            Listed.factory(
                self.session,
                issuer_domicile_code=self.issuer_domicile_code,
                issuer_name=self.issuer_name,
                ticker=self.ticker1,
                mic=self.mic)
        # Test retrieval on MIC, Ticker pair fails Exchange.mic argument
        with self.assertRaises(FactoryError):
            Listed.factory(
                self.session,
                issuer_domicile_code=self.issuer_domicile_code,
                issuer_name=self.issuer_name,
                ticker=self.ticker,
                mic='BAD_MIC')

    def test_factory_no_create(self):
        """Test create parameter."""
        with self.assertRaises(FactoryError):
            Listed.factory(
                self.session, self.isin, self.mic, self.ticker, self.name,
                self.issuer_domicile_code, self.issuer_name,
                create=False)

    def test_from_data_frame(self):
        """Get data from a pandas.DataFrame."""
        # Insert selected sub-set securities meta-data
        Listed.from_data_frame(
            self.session, data_frame=self.securities_dataframe)
        # Test one Listed instance
        listed = Listed.factory(self.session, self.isin)
        # Attributes
        self.assertIsInstance(listed, Listed)
        self.assertEqual(listed.name, self.name)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)

    def test_to_data_frame(self):
        """Convert class data attributes into a factory compatible dataframe."""
        # Insert selected sub-set securities meta-data
        Listed.from_data_frame(
            self.session, data_frame=self.securities_dataframe)
        # Method to be tested
        df = Listed.to_data_frame(self.session)
        # Test data
        test_df = self.securities_dataframe.copy()
        # Test
        df.sort_values(by='isin', inplace=True)
        df.reset_index(drop=True, inplace=True)
        test_df.sort_values(by='isin', inplace=True)
        test_df.reset_index(drop=True, inplace=True)
        test_df = test_df[df.columns]  # Align columns rank
        pd.testing.assert_frame_equal(test_df, df)

    def test_update_all(self):
        """Update all Listed instances from a getter method."""
        # Insert all securities meta-data (for all securities)
        Listed.update_all(self.session, self.get_meta_method)
        # Test one Listed instance
        listed = Listed.factory(self.session, self.isin)
        # Attributes
        self.assertIsInstance(listed, Listed)
        self.assertEqual(listed.name, self.name)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)

    def test_0_dump(self):
        """Dump all class instances and their time series data to disk."""
        # Dumper
        dumper = Dump(testing=True)
        # For testing delete old any test dump folder and re-create it empty
        dumper.delete(delete_folder=True)  # Delete dump folder and contents
        dumper.makedir()
        # Insert only selected subset of securities meta-data
        # Update all data instances: Listed & ListedEOD. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        Listed.update_all(  # Method to be tested
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            _test_isin_list=[self.isin, self.isin1, self.isin2])
        # Methods to be tested
        Listed.dump(self.session, dumper)
        # Verify dump files exists.
        self.assertTrue(
            dumper.exists(Listed), 'Listed dump file not found.')
        self.assertTrue(
            dumper.exists(ListedEOD), 'ListedEOD dump file not found.')

    def test_1_reuse(self):
        """Reuse dumped data as a database initialization resource.

        This test must run after ``test_0_dump`` so that the dump file exists
        and that the time series dates are correct; which is why there is a
        numeric part to the test names; as `unittest` sorts tests by test method
        name.
        """
        # Dumper
        dumper = Dump(testing=True)
        Listed.reuse(self.session, dumper)

        # Test one Listed instance
        listed = Listed.factory(self.session, self.isin)
        # Attributes
        self.assertIsInstance(listed, Listed)
        self.assertEqual(listed.name, self.name)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)

        # Retrieve the submitted ListedEOD data from asset_base
        df = pd.DataFrame([self.to_eod_dict(item)
                           for item in self.session.query(ListedEOD).all()])
        # Test
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.sort_values(['date_stamp', 'ticker'], inplace=True)
        # Test against last date test_values data
        last_date = pd.to_datetime(self.to_date)
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)
        # Test security time series last date
        securities_list = self.session.query(ListedEquity).all()
        self.assertTrue(
            all(x.get_last_eod_date() == last_date for x in securities_list))

    def test_key_code_id_table(self):
        """A table of all instance's ``Entity.id`` against ``key_code``."""
        # Insert all securities meta-data (for all securities)
        Listed.update_all(self.session, self.get_meta_method)
        instances_list = self.session.query(Listed).all()
        test_df = pd.DataFrame(
            [(item.id, item.key_code) for item in instances_list],
            columns=['id', 'isin'])
        df = Listed.key_code_id_table(self.session)
        pd.testing.assert_frame_equal(test_df, df)

    def test_update_all_trade_eod(self):
        """Update all Listed and ListedEOD objs from their getter methods."""
        # Insert only selected subset of securities meta-data
        # Update all data instances: Listed & ListedEOD. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        Listed.update_all(  # Method to be tested
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            _test_isin_list=[self.isin, self.isin1, self.isin2])
        # Retrieve the submitted ListedEOD data from asset_base
        df = pd.DataFrame([self.to_eod_dict(item)
                           for item in self.session.query(ListedEOD).all()])
        # Test
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.sort_values(['date_stamp', 'ticker'], inplace=True)
        # Test against last date test_values data
        last_date = pd.to_datetime(self.to_date)
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)
        # Test security time series last date
        # Securities asset_base instances list
        securities_list = self.session.query(ListedEquity).all()
        self.assertTrue(
            all(x.get_last_eod_date() == last_date for x in securities_list))

    def test_get_eod(self):
        """Return the EOD time series for the asset."""
        # Insert only selected subset of Listed instances from meta-data
        # Listed.from_data_frame(
        #     self.session, data_frame=self.selected_securities_dataframe)
        # Update all data instances: Listed & ListedEOD. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        Listed.update_all(
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            _test_isin_list=[self.isin, self.isin1, self.isin2])
        # Test AAPL Inc.
        listed = Listed.factory(self.session, self.isin)
        listed1 = Listed.factory(self.session, self.isin1)
        listed2 = Listed.factory(self.session, self.isin2)
        # Method to be tested
        df = listed.get_eod()
        df1 = listed1.get_eod()
        df2 = listed2.get_eod()
        # Make to-test data
        df.reset_index(inplace=True)
        df1.reset_index(inplace=True)
        df2.reset_index(inplace=True)
        df['ticker'] = listed.ticker
        df1['ticker'] = listed1.ticker
        df2['ticker'] = listed2.ticker
        df = pd.concat([df, df1, df2], axis='index')
        # Test
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.sort_values(['date_stamp', 'ticker'], inplace=True)
        # Test against last date test_values data
        last_date = pd.to_datetime(self.to_date)
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)

    def test_get_last_eod(self):
        """Return the EOD last date, data dict, for the asset.

        Note
        ----
        This test relies heavily on ``test_get_eod_trade_series`` passing.
        """
        # Update all data instances: Listed & ListedEOD. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        Listed.update_all(
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            _test_isin_list=[self.isin, self.isin1, self.isin2])
        # Test for AAPL Inc.
        listed = Listed.factory(self.session, self.isin)
        # Method to be tested
        last_dict = listed.get_last_eod()
        # Test values
        eod = listed.get_eod()
        eod.reset_index(inplace=True)
        last_eod = eod.iloc[-1]
        last_eod['date_stamp'] = last_eod['date_stamp'].to_pydatetime().date()
        last_dict_test = last_eod.to_dict()
        self.assertIsInstance(last_dict, dict)
        self.assertIsInstance(last_dict_test, dict)
        self.assertEqual(last_dict, last_dict_test)

    def test_get_live_trades(self):
        """Return live trade data if available else use the last EOD trades."""
        # TODO: TBDL. Not in a functioning state.
        pass

    def test__check_isin(self):
        """Check to see if the isin number provided is valid."""
        # Create Issuer instance here as it should be created during testing
        # only as later on the Listed.factory method will be tested to create
        # the issuer from it's arguments.
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        listed = Listed(
            self.name,  issuer, self.isin, self.exchange, self.ticker)
        # Assert a chosen isin to be identical to test data
        test_isin = 'US0378331005'
        self.assertEqual(self.isin, test_isin)
        # Test ISIN
        listed._check_isin(test_isin)
        # Test Bad ISIN
        bad_isin = 'US0378331006'  # Test ISIN last digit modified
        with self.assertRaises(BadISIN):
            listed._check_isin(bad_isin)


class TestListedEquity(TestListed):
    """Test ListedEquity and IndustryClassICB classes."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = ListedEquity
        # Securities EOD-data
        cls.get_dividends_method = History().get_dividends
        # ICB Classification
        cls.industry_class = 'icb'
        cls.industry_name = 'Exchange Traded Funds'
        cls.super_sector_name = 'Exchange Traded Products'
        cls.sector_name = 'Exchange Traded Funds'
        cls.sub_sector_name = 'Exchange Traded Funds'
        cls.industry_code = 'A140'
        cls.super_sector_code = 'A300'
        cls.sector_code = 'A310'
        cls.sub_sector_code = 'A311'

        # Additional Test data for dividends form the TestDividend test class.
        # Remember the Trade EOD test data is inherited form the parent class.
        cls.div_from_date = '2020-01-01'
        cls.div_to_date = '2020-12-31'
        cls.div_columns = [
            'date_stamp', 'ticker', 'mic', 'isin',
            'currency', 'declaration_date', 'payment_date', 'period',
            'record_date', 'unadjusted_value', 'adjusted_value']
        # FIXME: Why is ZAC the currency, check the dividends history!!
        cls.div_test_df = pd.DataFrame([  # Last 3 dividends
            ['2020-10-21', 'STX40', 'XJSE', 'ZAE000027108', 'ZAC',
                None,         None,        None,         None, 9.1925, 9.1925],
            ['2020-11-06', 'AAPL',  'XNYS', 'US0378331005', 'USD', '2020-10-29',
                '2020-11-12', 'Quarterly', '2020-11-09', 0.2050, 0.2050],
            ['2020-11-30', 'MCD',   'XNYS', 'US5801351017', 'USD', '2020-10-08',
                '2020-12-15', 'Quarterly', '2020-12-01', 1.2900, 1.2900]],
            columns=cls.div_columns)

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Insert selected sub-set securities meta-data

    def to_dividend_dict(self, item):
        """Convert all class price attributes to a dictionary."""
        data = {
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

        return data

    def test___init__(self):
        """Initialization."""
        # Create Issuer instance here as it should be created during testing
        # only as later on the Listed.factory method will be tested to create
        # the issuer from it's arguments.
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)

        # Test without industry classification
        listed = ListedEquity(
            self.name, issuer, self.isin, self.exchange, self.ticker,
            status=self.status)
        self.assertIsInstance(listed, ListedEquity)
        self.assertEqual(listed.name, self.name)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)

        # Test with industry classification
        listed = ListedEquity(
            self.name,  issuer, self.isin, self.exchange, self.ticker,
            status=self.status,
            industry_class='icb',
            industry_name=self.industry_name,
            super_sector_name=self.super_sector_name,
            sector_name=self.sector_name,
            sub_sector_name=self.sub_sector_name,
            industry_code=self.industry_code,
            super_sector_code=self.super_sector_code,
            sector_code=self.sector_code,
            sub_sector_code=self.sub_sector_code,)
        self.assertIsInstance(listed, ListedEquity)
        self.assertEqual(listed.name, self.name)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)
        # Check industry classification info
        self.assertEqual(listed.industry_class, self.industry_class)
        icb = listed.industry_class_instance
        self.assertEqual(icb.industry_name, self.industry_name)
        self.assertEqual(icb.super_sector_name, self.super_sector_name)
        self.assertEqual(icb.sector_name, self.sector_name)
        self.assertEqual(icb.sub_sector_name, self.sub_sector_name)
        self.assertEqual(icb.industry_code, self.industry_code)
        self.assertEqual(icb.super_sector_code, self.super_sector_code)
        self.assertEqual(icb.sector_code, self.sector_code)
        self.assertEqual(icb.sub_sector_code, self.sub_sector_code)

        # Test class the polymorphism functionality.
        # Query the superclass Asset which should produce a Cash polymorphic
        # instance
        instances = self.session.query(Asset).all()  # There are two, see above!
        instance1, instance2 = instances
        self.assertEqual(instance1._class_name, 'ListedEquity')
        self.assertEqual(instance1._discriminator, 'listed_equity')
        self.assertEqual(instance2._class_name, 'ListedEquity')
        self.assertEqual(instance2._discriminator, 'listed_equity')


    def test_factory(self):
        """Full suite of factory parameters with previously existing issuer."""
        # Add. Issuer should be automatically created.
        listed = ListedEquity.factory(
            self.session, self.isin, self.mic, self.ticker, self.name,
            self.issuer_domicile_code, self.issuer_name, self.status,
            industry_class='icb',
            industry_name=self.industry_name,
            super_sector_name=self.super_sector_name,
            sector_name=self.sector_name,
            sub_sector_name=self.sub_sector_name,
            industry_code=self.industry_code,
            super_sector_code=self.super_sector_code,
            sector_code=self.sector_code,
            sub_sector_code=self.sub_sector_code,)
        # Inspect database for expected number of entities
        self.assertEqual(len(self.session.query(Issuer).all()), 1)
        self.assertEqual(len(self.session.query(ListedEquity).all()), 1)
        # Different query argument sets produce the same instance
        # Firstly buy ISIN
        listed1 = ListedEquity.factory(self.session, isin=self.isin)
        self.assertEqual(listed, listed1)
        # Secondly by (MIC,ticker)
        listed2 = ListedEquity.factory(
            self.session, mic=self.mic, ticker=self.ticker)
        self.assertEqual(listed, listed2)
        # Attributes
        self.assertEqual(listed.name, self.name)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN country code, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)
        # Check industry classification info
        self.assertEqual(listed.industry_class, self.industry_class)
        icb = listed.industry_class_instance
        self.assertEqual(icb.industry_name, self.industry_name)
        self.assertEqual(icb.super_sector_name, self.super_sector_name)
        self.assertEqual(icb.sector_name, self.sector_name)
        self.assertEqual(icb.sub_sector_name, self.sub_sector_name)
        self.assertEqual(icb.industry_code, self.industry_code)
        self.assertEqual(icb.super_sector_code, self.super_sector_code)
        self.assertEqual(icb.sector_code, self.sector_code)
        self.assertEqual(icb.sub_sector_code, self.sub_sector_code)

    def test_from_data_frame(self):
        """Get data from a pandas.DataFrame."""
        # Insert selected sub-set securities meta-data
        ListedEquity.from_data_frame(
            self.session, data_frame=self.securities_dataframe)
        # Test one ListedEquity instance
        listed = ListedEquity.factory(self.session, self.isin)
        # Attributes
        self.assertIsInstance(listed, ListedEquity)
        self.assertEqual(listed.name, self.name)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)
        # Check industry classification info
        self.assertEqual(listed.industry_class, self.industry_class)
        icb = listed.industry_class_instance
        self.assertEqual(icb.industry_name, self.industry_name)
        self.assertEqual(icb.super_sector_name, self.super_sector_name)
        self.assertEqual(icb.sector_name, self.sector_name)
        self.assertEqual(icb.sub_sector_name, self.sub_sector_name)
        self.assertEqual(icb.industry_code, self.industry_code)
        self.assertEqual(icb.super_sector_code, self.super_sector_code)
        self.assertEqual(icb.sector_code, self.sector_code)
        self.assertEqual(icb.sub_sector_code, self.sub_sector_code)

    def test_to_data_frame(self):
        """Convert class data attributes into a factory compatible dataframe."""
        # Insert selected sub-set securities meta-data
        ListedEquity.from_data_frame(
            self.session, data_frame=self.securities_dataframe)
        # Method to be tested
        df = ListedEquity.to_data_frame(self.session)
        # Test data
        test_df = self.securities_dataframe.copy()
        # Test
        df.sort_values(by='isin', inplace=True)
        df.reset_index(drop=True, inplace=True)
        test_df.sort_values(by='isin', inplace=True)
        test_df.reset_index(drop=True, inplace=True)
        test_df = test_df[df.columns]  # Align columns rank
        pd.testing.assert_frame_equal(test_df, df)

    def test_update_all(self):
        """Update all Listed instances from a getter method."""
        # Insert all securities meta-data (for all securities)
        ListedEquity.update_all(self.session, self.get_meta_method)
        # Test one ListedEquity instance
        listed = ListedEquity.factory(self.session, self.isin)
        # Attributes
        self.assertIsInstance(listed, ListedEquity)
        self.assertEqual(listed.name, self.name)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)
        # Check industry classification info
        self.assertEqual(listed.industry_class, self.industry_class)
        icb = listed.industry_class_instance
        self.assertEqual(icb.industry_name, self.industry_name)
        self.assertEqual(icb.super_sector_name, self.super_sector_name)
        self.assertEqual(icb.sector_name, self.sector_name)
        self.assertEqual(icb.sub_sector_name, self.sub_sector_name)
        self.assertEqual(icb.industry_code, self.industry_code)
        self.assertEqual(icb.super_sector_code, self.super_sector_code)
        self.assertEqual(icb.sector_code, self.sector_code)
        self.assertEqual(icb.sub_sector_code, self.sub_sector_code)

    def test_0_dump(self):
        """Dump all class instances and their time series data to disk."""
        # Dumper
        dumper = Dump(testing=True)
        # For testing delete old any test dump folder and re-create it empty
        dumper.delete(delete_folder=True)  # Delete dump folder and contents
        dumper.makedir()
        # Insert only selected subset of securities meta-data. Update all data
        # instances: ListedEquity, ListedEOD & Dividend. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        ListedEquity.update_all(  # Method to be tested
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            get_dividends_method=self.get_dividends_method,
            _test_isin_list=[self.isin, self.isin1, self.isin2])
        # Methods to be tested
        ListedEquity.dump(self.session, dumper)
        # Verify dump files exists.
        self.assertTrue(
            dumper.exists(ListedEquity), 'Listed dump file not found.')
        self.assertTrue(
            dumper.exists(ListedEOD), 'ListedEOD dump file not found.')
        self.assertTrue(
            dumper.exists(Dividend), 'Dividend dump file not found.')

    def test_1_reuse(self):
        """Reuse dumped data as a database initialization resource.

        This test must run after ``test_0_dump`` so that the dump file exists
        and that the time series dates are correct; which is why there is a
        numeric part to the test names; as `unittest` sorts tests by test method
        name.
        """
        # Dumper
        dumper = Dump(testing=True)
        ListedEquity.reuse(self.session, dumper)

        # Test one ListedEquity instance
        listed = ListedEquity.factory(self.session, self.isin)
        # Attributes
        self.assertIsInstance(listed, ListedEquity)
        self.assertEqual(listed.name, self.name)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN, against issuer arguments.
        self.assertEqual(listed.domicile.country_code, self.isin[0:2])
        self.assertEqual(listed.domicile.country_code,
                         self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)

        # Retrieve the submitted ListedEOD data from asset_base
        df = pd.DataFrame([self.to_eod_dict(item)
                           for item in self.session.query(ListedEOD).all()])
        # Test
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.sort_values(['date_stamp', 'ticker'], inplace=True)
        # Test against last date test_values data
        last_date = pd.to_datetime(self.to_date)
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)

        df = pd.DataFrame([self.to_dividend_dict(item)
                           for item in self.session.query(Dividend).all()])
        df.sort_values(by='date_stamp', inplace=True)
        # Test over test-date-range
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.set_index('date_stamp', inplace=True)
        df = df.loc[self.div_from_date:self.div_to_date]
        df.reset_index(inplace=True)
        # Test
        self.assertEqual(len(df), 12)
        df.reset_index(inplace=True, drop=True)
        self.assertEqual(set(df.columns), set(self.div_columns))
        # Test against last 3 dividends
        df = df.iloc[-3:].reset_index(drop=True)  # Make index 0, 1, 2
        date_to_str(df)  # Convert Timestamps
        df.replace({pd.NaT: None}, inplace=True)  # Replace pandas NaT with None
        self.assertTrue(
            df.sort_index(axis='columns').equals(
                self.div_test_df.sort_index(axis='columns')),
            'Dividend test data mismatch')

    def test_update_all_trade_eod_and_dividends(self):
        """Update all Listed, ListedEOD, Dividend objs from getter methods."""
        # Insert only selected subset of securities meta-data. Update all data
        # instances: ListedEquity, ListedEOD & Dividend. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        ListedEquity.update_all(  # Method to be tested
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            get_dividends_method=self.get_dividends_method,
            _test_isin_list=[self.isin, self.isin1, self.isin2])

        # Retrieve the submitted ListedEOD data from asset_base
        df = pd.DataFrame([self.to_eod_dict(item)
                           for item in self.session.query(ListedEOD).all()])
        # Test
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.sort_values(['date_stamp', 'ticker'], inplace=True)
        # Test against last date test_values data
        last_date = pd.to_datetime(self.to_date)
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)

        df = pd.DataFrame([self.to_dividend_dict(item)
                           for item in self.session.query(Dividend).all()])
        df.sort_values(by='date_stamp', inplace=True)
        # Test over test-date-range
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.set_index('date_stamp', inplace=True)
        df = df.loc[self.div_from_date:self.div_to_date]
        df.reset_index(inplace=True)
        # Test
        self.assertEqual(len(df), 12)
        df.reset_index(inplace=True, drop=True)
        self.assertEqual(set(df.columns), set(self.div_columns))
        # Test against last 3 dividends
        df = df.iloc[-3:].reset_index(drop=True)  # Make index 0, 1, 2
        date_to_str(df)  # Convert Timestamps
        df.replace({pd.NaT: None}, inplace=True)  # Replace pandas NaT with None
        self.assertTrue(
            df.sort_index(axis='columns').equals(
                self.div_test_df.sort_index(axis='columns')),
            'Dividend test data mismatch')

    def test_get_dividend_series(self):
        """Return the EOD trade data series for the security."""
        # Insert only selected subset of securities meta-data. Update all data
        # instances: ListedEquity, ListedEOD & Dividend. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        ListedEquity.update_all(  # Method to be tested
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            get_dividends_method=self.get_dividends_method,
            _test_isin_list=[self.isin, self.isin1, self.isin2])
        # Method to be tested
        df = ListedEquity.factory(
            self.session, self.isin).get_dividend_series()
        df1 = ListedEquity.factory(
            self.session, self.isin1).get_dividend_series()
        df2 = ListedEquity.factory(
            self.session, self.isin2).get_dividend_series()
        df = pd.concat([df, df1, df2], axis='index')
        # Test over test-date-range
        test_df = self.div_test_df.copy()
        df.sort_index(inplace=True)
        df = df.loc[self.from_date:self.to_date]
        df = df.iloc[-3:]  # Test data is for last three
        df.reset_index(inplace=True)
        test_df = test_df[df.columns]
        # Make dates all strings for simple testing.
        date_to_str(df)  # Convert Timestamps
        # Test
        pd.testing.assert_frame_equal(test_df, df)

    def test_time_series(self):
        """Retrieve historic time-series"""
        # Insert only selected subset of securities meta-data. Update all data
        # instances: ListedEquity, ListedEOD & Dividend. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        ListedEquity.update_all(  # Method to be tested
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            get_dividends_method=self.get_dividends_method,
            _test_isin_list=[self.isin2])
        # Test for AAPL Inc.
        listed = ListedEquity.factory(self.session, self.isin2)
        # Method to be tested
        # Check data
        # NOTE: These values may change as EOD historical data gets corrected
        last_date = pd.to_datetime(self.to_date)
        price = listed.time_series()
        self.assertEqual(price[last_date], 54.60)
        volume = listed.time_series(series='volume')
        self.assertEqual(volume[last_date], 112700)
        dividend = listed.time_series(series='dividend')
        self.assertEqual(dividend.loc['2020-10-21'], 0.091925)
        total_price = listed.time_series(return_type='total_price')
        self.assertEqual(total_price[last_date], 81.24353503753133)


class TestIndex(TestBase):
    """An index representing some financial data."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Index
        # Get methods
        cls.get_method = MetaData().get_indices
        cls.get_eod_method = History().get_indices
        # Test strings
        cls.index_name = 'Gold Spot'
        cls.ticker = 'XYZ'
        cls.test_str = 'Gold Spot is an Index priced in USD.'
        cls.key_code = 'XYZ'
        cls.identity_code = 'XYZ'
        # Small set of test index tickers
        cls.index_tickers = ('GSPC', 'ASX', 'J200')
        # Test ListedEOD time series data
        # Test data
        cls.from_date = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d').date()
        cls.to_date = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d').date()
        cls.columns = [
            'date_stamp', 'ticker', 'mic', 'isin',
            'adjusted_close', 'close', 'high', 'low', 'open']
        cls.test_columns = [
            'adjusted_close', 'close', 'high', 'low', 'open']
        # Exclude adjusted_close as it changes
        cls.test_values = pd.DataFrame([  # Last date data
            [3673.63, 3673.63, 3723.98, 3664.69, 3723.98],
            [3756.0701, 3756.0701, 3760.2, 3726.8799, 3733.27],
            [54379.58, 54379.58, 54615.33, 53932.88, 54615.33]
        ], columns=cls.test_columns)

    def test___init__(self):
        """Initialization."""
        index = Index(self.index_name, self.ticker, self.currency, True)
        self.assertIsInstance(index, Index)
        self.assertTrue(index.total_return)

    def test___str__(self):
        index = Index(self.index_name, self.ticker, self.currency)
        self.assertEqual(self.test_str, index.__str__())

    def test_key_code(self):
        index = Index(self.index_name, self.ticker, self.currency)
        self.assertEqual(self.key_code, index.key_code)

    def test_identity_code(self):
        index = Index(self.index_name, self.ticker, self.currency)
        self.assertEqual(self.identity_code, index.identity_code)

    def test_factory(self):
        """Manufacture/retrieve an instance from the given parameters."""
        # Add.
        index = Index.factory(
            self.session, self.index_name, self.ticker, self.currency_ticker)
        index = Index.factory(
            self.session, self.index_name, self.ticker, self.currency_ticker)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Index).all()), 1)
        # Attributes
        self.assertEqual(index.name, self.index_name)
        self.assertEqual(index.currency_ticker, self.currency_ticker)
        # Get same
        indx1 = Index.factory(
            self.session, self.index_name, self.ticker, self.currency_ticker)
        self.assertEqual(index, indx1)

    def test_update_all(self):
        """Update/create all the objects in the asset_base session."""
        # Test a limited number of test tickers
        Index.update_all(
            self.session, self.get_method, self.get_eod_method,
            _test_ticker_list=self.index_tickers)
        # Make sure they are all present
        index_list = self.session.query(Index).all()
        ticker_list = [index.ticker for index in index_list]
        self.assertEqual(set(self.index_tickers), set(ticker_list))
        # Retrieve the submitted ListedEOD data from asset_base
        df = pd.DataFrame([item.to_dict()
                           for item in self.session.query(IndexEOD).all()])
        # Test
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.sort_values(['date_stamp'], inplace=True)
        # Test against last date test_values data
        last_date = pd.to_datetime(self.to_date)
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        # Sort to remove ambiguity
        df.sort_values(by='close', inplace=True)
        df.reset_index(drop=True, inplace=True)
        self.test_values.sort_values(by='close', inplace=True)
        self.test_values.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)


class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # Classes that are passing. Add the others later when they too work.
        test_classes = [
            TestAsset,
            TestCash,
            TestForex,
            TestShare,
            TestListed,
            TestListedEquity,
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
