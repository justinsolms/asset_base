import unittest
import sys
import datetime
import pandas as pd
import pandas

from asset_base.financial_data import Static

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from asset_base.common import TestSession
from asset_base.exceptions import FactoryError, ReconcileError
from asset_base.entity import Base
from asset_base.entity import Currency, Domicile
from asset_base.entity import Entity, Issuer, Exchange

from asset_base.asset import Asset

class TestAsset(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Asset

        # Domicile data
        cls.get_method = Static().get_domicile
        cls.domicile_dataframe = cls.get_method()
        # A single domicile with currency
        cls.domicile_item = cls.domicile_dataframe[
            cls.domicile_dataframe.country_code == 'US']
        cls.country_code = cls.domicile_item.country_code.to_list()[0]
        cls.country_name = cls.domicile_item.country_name.to_list()[0]
        cls.currency_ticker = cls.domicile_item.currency_ticker.to_list()[0]
        # Test strings
        cls.name = 'Test Asset'
        cls.test_str = 'Test Asset is an entity in United States'
        cls.key_code = 'US.Test Asset'
        cls.identity_code = 'US.Test Asset'

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean sqlite in-memory database
        self.session = TestSession().session
        # Add all Currency objects to entitybase
        Currency.update_all(self.session, get_method=Static().get_currency)
        # Add all Domicile objects to the entitybase
        # Domicile.update_all(self.session, get_method=Static().get_domicile)
        # self.domicile = Domicile.factory(self.session, self.country_code)

    def test___init__(self):
        pass
        # asset = Asset(self.name, self.currency)
        # self.assertIsInstance(asset, Asset)

    def test_factory(self):
        """Test session add asset but domicile and currency already added."""
        # FIXME: Drop test. We needed it only for a bug we had.
        # Pre-add currency.
        # Add.
        asset = Asset.factory(self.session, self.name, self.country_code)
        asset = Asset.factory(self.session, self.name, self.country_code)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Asset).all()), 1)
        # Attributes
        self.assertEqual(asset.name, self.name)
        self.assertEqual(asset.domicile.code, self.domicile.code)
        # Get
        asset1 = Asset.factory(self.session, self.name, self.country_code)
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
        # Test strings
        cls.currency_code = 'USD'
        cls.currency_name = 'United States'
        cls.test_str = 'Cash USD in United States'
        cls.key_code = 'US.USD'
        cls.identity_code = 'US.USD'
        cls.name = 'U.S. Dollar'
        # The convention used by this module is to use yesterday's close price
        # due to the limitation imposed by the database price feed.
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        # Round off the date to remove the time and keep only the date
        # component.
        date_stamp = datetime.date(
            yesterday.year, yesterday.month, yesterday.day)
        cls.test_price_dict = {'date_stamp': date_stamp, "close": 1.0}

    def test___init__(self):
        domicile = Domicile.factory(self.session, self.domicile.code)
        cash = Cash(domicile)
        self.assertIsInstance(cash, Cash)
        self.assertEqual(cash.domicile, domicile)
        self.assertEqual(cash.name, self.name)
        self.assertEqual(cash.ticker, self.currency_code)

    def test___str__(self):
        domicile = Domicile.factory(self.session, self.domicile.code)
        cash = Cash(domicile)
        self.assertEqual(self.test_str, cash.__str__())

    def test_key_code(self):
        domicile = Domicile.factory(self.session, self.domicile.code)
        cash = Cash(domicile)
        self.assertEqual(self.key_code, cash.key_code)

    def test_identity_code(self):
        domicile = Domicile.factory(self.session, self.domicile.code)
        cash = Cash(domicile)
        self.assertEqual(self.identity_code, cash.identity_code)

    def test_factory(self):
        """Test session add entity with domicile and currency already added."""
        # Add.
        Cash.factory(self.session, self.country_code)
        # Retrieve.
        cash = Cash.factory(self.session, self.country_code)
        # Test
        self.assertIsInstance(cash, Cash)
        self.assertEqual(cash.domicile.code, self.country_code)
        self.assertEqual(cash.name, self.name)
        self.assertEqual(cash.ticker, self.currency_code)

    def test_factory_fail(self):
        """Test session add fail if second add has wrong domicile_name."""
        with self.assertRaises(FactoryError):
            Cash.factory(self.session, 'WRONG')

    def test_update_all(self):
        """Create all ``Cash`` instances form all ``Domicile`` instances."""
        Cash.update_all(self.session)
        # Test data
        domiciles = self.session.query(Domicile).all()
        test_codes = list(set(item.currency.code for item in domiciles))
        test_codes.sort()
        # Test Cash
        cash = self.session.query(Cash).all()
        codes = [item.ticker for item in cash]
        codes.sort()
        # Assert
        self.assertEqual(codes, test_codes)

    def test_get_last_eod_trades(self):
        """Return the last EOD price data set in the history."""
        # Add.
        Cash.factory(self.session, self.country_code)
        # Retrieve.
        cash = Cash.factory(self.session, self.country_code)
        # Tested method
        price_dict = cash.get_last_eod_trades()
        # Test
        self.assertEqual(price_dict, self.test_price_dict)

    def test_time_series(self):
        """Retrieve historic time-series for a set of class instances."""
        domicile = Domicile.factory(self.session, self.domicile.code)
        cash = Cash(domicile)
        # Simulate instrument price data date index
        date_index = pd.date_range(start='2000/01/01', periods=10)
        # Test the method
        time_series = cash.time_series(date_index=date_index)
        # Test
        self.assertTrue((time_series.index == date_index).all())
        self.assertTrue((time_series == 1.0).all())
        # Check Cash name
        self.assertEqual(time_series.name.identity_code, cash.identity_code)

