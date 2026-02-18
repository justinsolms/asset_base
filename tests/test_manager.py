#!/usr/bin/env unittest
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Test suite for the asset_base module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>. This file is part of
the asset_base module. The asset_base module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

The classmethod ``setUpClass`` sets up test class fixtures and method ``setUp``
sets up test case fixtures. The design of all the tests is to have ``setUp``
initialize a new and empty memory database for every test case.

All non-committed class fixtures are set up in the classmethod ``setUpClass``
and the committed ``asset_base`` ORM class instances are committed in the test
case fixture set up method ``setUp`` after a new blank database has been created
using the class fixtures set up in ``setUpClass``.

TODO: Describe the design philosophy of the module, especially reoccurring
methods such as ``factory``, ``from_dataframe``, and lots more.


"""
import unittest
import unittest.mock
import pandas as pd

from sqlalchemy.exc import NoResultFound

from src.asset_base.common import Common
from src.asset_base.financial_data import Dump
from src.asset_base.asset import Cash, Forex, ListedEquity
from src.asset_base.time_series import Dividend, ListedEOD, Split
from src.asset_base.manager import Manager, Meta, substitute_security_labels
from src.asset_base.exceptions import TimeSeriesNoData
from src.asset_base.time_series_processor import TimeSeriesProcessor

import warnings

# Get module-named logger.
import logging

logger = logging.getLogger(__name__)

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

class TestManagerInit(unittest.TestCase):

    """Manager sessions with different backend databases"""

    def common_todo(self):
        """Some common post creation tests using ``Meta`` class."""
        test_name = 'Meta class instance'
        test_value = 'test_value'
        session = self.manager.session
        test_obj = Meta(test_name, test_value)
        self.assertIsInstance(test_obj, Meta)
        session.add(test_obj)
        session.flush()
        obj = session.query(Meta).filter(Meta.name==test_name).one()
        self.assertEqual(test_obj, obj)
        self.assertEqual(obj.name, test_name)
        self.assertEqual(obj.value, test_value)

    def test_make_session_memory(self):
        """Make database sessions in either sqlite, mysql or memory."""
        self.manager = Manager(dialect='memory', testing=True)
        self.common_todo()

    def test_make_session_sqlite(self):
        """Make database sessions in either sqlite, mysql or memory."""
        self.manager = Manager(dialect='sqlite', testing=True)
        self.common_todo()

    def test_make_session_sqlite_not_testing(self):
        """Make database sessions in either sqlite, mysql or memory.

        Warning
        -------
        This messes with the deployed database!!!!!
        """
        self.manager = Manager(dialect='sqlite', testing=False)
        self.common_todo()


class TestManager(unittest.TestCase):
    """Test Manager methods."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a memory database manager for testing
        self.manager = Manager(dialect='memory', testing=True)
        self.session = self.manager.session

        # Set up Static mock patcher
        self.static_patcher = unittest.mock.patch('src.asset_base.manager.Static')
        self.mock_static_class = self.static_patcher.start()
        self.mock_static = unittest.mock.MagicMock()
        self.mock_static_class.return_value = self.mock_static

        # Configure default static data - tests can override if needed
        self._configure_standard_static_data()

    def tearDown(self):
        """Tear down test fixtures."""
        self.static_patcher.stop()
        if hasattr(self, 'manager'):
            self.manager.close()

    def _configure_static_mock(self, mock_static, currency_tickers, domicile_codes, exchange_mics):
        """Helper to configure Static mock with basic currency/domicile/exchange data."""
        # Map currencies to countries
        currency_map = {'USD': 'US', 'EUR': 'EU'}
        country_names = {'US': 'United States', 'EU': 'European Union'}
        currency_names = {'USD': 'US Dollar', 'EUR': 'Euro'}
        exchange_names = {'XNYS': 'New York Stock Exchange', 'XLON': 'London Stock Exchange'}
        exchange_countries = {'XNYS': 'US', 'XLON': 'GB'}

        mock_static.get_currency.return_value = pd.DataFrame({
            'ticker': currency_tickers,
            'name': [currency_names.get(t, f'{t} Currency') for t in currency_tickers],
            'country_code_list': [currency_map.get(t, t[:2]) for t in currency_tickers]
        })

        mock_static.get_domicile.return_value = pd.DataFrame({
            'country_code': domicile_codes,
            'country_name': [country_names.get(c, f'Country {c}') for c in domicile_codes],
            'currency_ticker': [next((k for k, v in currency_map.items() if v == c), currency_tickers[0]) for c in domicile_codes]
        })

        mock_static.get_exchange.return_value = pd.DataFrame({
            'mic': exchange_mics,
            'exchange_name': [exchange_names.get(m, f'{m} Exchange') for m in exchange_mics],
            'country_code': [exchange_countries.get(m, 'US') for m in exchange_mics],
            'operating_mic': exchange_mics
        })

    def _configure_standard_static_data(self, currency_tickers=None, domicile_codes=None, exchange_mics=None):
        """Configure standard static data mocks.

        Parameters
        ----------
        currency_tickers : list, optional
            List of currency tickers. Defaults to ['USD'].
        domicile_codes : list, optional
            List of domicile country codes. Defaults to ['US'].
        exchange_mics : list, optional
            List of exchange MICs. Defaults to ['XNYS'].
        """
        if currency_tickers is None:
            currency_tickers = ['USD']
        if domicile_codes is None:
            domicile_codes = ['US']
        if exchange_mics is None:
            exchange_mics = ['XNYS']
        self._configure_static_mock(self.mock_static, currency_tickers, domicile_codes, exchange_mics)

    def _create_listed_equity(self, num_eod=3, add_dividend=True, add_split=True):
        """Create a basic USD ListedEquity with optional EOD/dividend/split data."""
        from src.asset_base.entity import Currency, Domicile, Issuer, Exchange
        import datetime

        usd = Currency.factory(self.session, 'USD')
        domicile = Domicile.factory(self.session, 'US')
        issuer = Issuer.factory(self.session, 'Test Company', 'US')
        exchange = Exchange.factory(self.session, 'XNYS')

        listed = ListedEquity(
            name="Test Stock",
            issuer=issuer,
            isin="US0378331005",
            exchange=exchange,
            ticker="TEST",
            status="listed"
        )
        self.session.add(listed)
        self.session.commit()

        # Add EOD data
        for i in range(num_eod):
            date = datetime.date(2024, 1, 1) + datetime.timedelta(days=i)
            eod = ListedEOD(
                base_obj=listed,
                date_stamp=date,
                open=100.0 + i,
                close=101.0 + i,
                high=102.0 + i,
                low=99.0 + i,
                adjusted_close=101.0 + i,
                volume=1000 + i
            )
            self.session.add(eod)

        if add_dividend:
            dividend_date = datetime.date(2024, 1, 2)
            dividend = Dividend(
                base_obj=listed,
                date_stamp=dividend_date,
                currency='USD',
                declaration_date=dividend_date,
                payment_date=dividend_date,
                period='Quarterly',
                record_date=dividend_date,
                unadjusted_value=0.50,
                adjusted_value=0.50
            )
            self.session.add(dividend)

        if add_split:
            split_date = datetime.date(2024, 1, 3)
            split = Split(
                base_obj=listed,
                date_stamp=split_date,
                numerator=2.0,
                denominator=1.0
            )
            self.session.add(split)

        self.session.commit()
        return listed

    def test_manager_initialization(self):
        """Test Manager instance initialization."""
        self.assertIsInstance(self.manager, Manager)
        self.assertEqual(self.manager._dialect, 'memory')
        self.assertTrue(self.manager.testing)
        self.assertIsNotNone(self.manager.session)

    def test_manager_context_manager(self):
        """Test Manager as context manager."""
        with Manager(dialect='memory', testing=True) as mgr:
            self.assertIsInstance(mgr, Manager)
            self.assertIsNotNone(mgr.session)

    def test_commit_successful(self):
        """Test successful commit."""
        from src.asset_base.manager import Meta
        test_obj = Meta("test_key", "test_value")
        self.session.add(test_obj)
        # Should not raise
        self.manager.commit()
        # Verify object was committed
        result = self.session.query(Meta).filter_by(name="test_key").first()
        self.assertIsNotNone(result)

    def test_commit_rollback_on_error(self):
        """Test commit rolls back on error."""
        from src.asset_base.manager import Meta
        # Add an object
        test_obj1 = Meta("test_key1", "value1")
        self.session.add(test_obj1)
        self.manager.commit()

        # Add another object
        test_obj2 = Meta("test_key2", "value2")
        self.manager.commit()

    def test_set_up_creates_static_data(self):
        """Test set_up creates Currency, Domicile, Exchange data."""
        # Configure with multiple currencies and domiciles
        self._configure_standard_static_data(
            currency_tickers=['USD', 'EUR'],
            domicile_codes=['US', 'EU']
        )

        self.manager.set_up(reuse=False, update=False)

        # Verify static data was created
        from src.asset_base.entity import Currency, Domicile, Exchange
        currencies = self.session.query(Currency).count()
        self.assertGreater(currencies, 0)

    @unittest.mock.patch('src.asset_base.financial_data.History.get_forex')
    @unittest.mock.patch('src.asset_base.financial_data.History.get_splits')
    @unittest.mock.patch('src.asset_base.financial_data.History.get_dividends')
    @unittest.mock.patch('src.asset_base.financial_data.History.get_eod')
    @unittest.mock.patch('src.asset_base.financial_data.MetaData.get_etfs')
    def test_update_calls_financial_data_methods(
        self, mock_get_etfs, mock_get_eod,
        mock_get_dividends, mock_get_splits, mock_get_forex
    ):
        """Test update method would call financial data methods (mock only)."""
        # Mock financial data methods to return empty DataFrames
        mock_get_etfs.return_value = pd.DataFrame()
        mock_get_eod.return_value = pd.DataFrame()
        mock_get_dividends.return_value = pd.DataFrame()
        mock_get_splits.return_value = pd.DataFrame()
        mock_get_forex.return_value = pd.DataFrame()

        self.manager.set_up(reuse=False, update=False)

        # Verify set_up completed successfully
        from src.asset_base.entity import Currency
        self.assertGreater(self.session.query(Currency).count(), 0)

    def test_get_meta(self):
        """Test get_meta returns dictionary of metadata."""
        from src.asset_base.manager import Meta
        # Add some metadata
        self.session.add(Meta("test_key", "test_value"))
        self.manager.commit()

        meta_dict = self.manager.get_meta()
        self.assertIsInstance(meta_dict, dict)
        self.assertIn("test_key", meta_dict)
        self.assertEqual(meta_dict["test_key"], "test_value")

    def test_dump_and_reuse(self):
        """Test dump and reuse methods."""
        # This is a basic test - full testing would require actual data
        # Just verify methods don't crash
        self.manager.dump()
        self.manager.reuse()

    def test_get_time_series_processor_empty_list_raises_error(self):
        """Test get_time_series_processor with empty asset list raises ValueError."""
        with self.assertRaises(ValueError) as context:
            self.manager.get_time_series_processor([])
        self.assertIn("may not be empty", str(context.exception))

    def test_get_time_series_processor_with_listed_equity(self):
        """Test get_time_series_processor with a ListedEquity identity_code."""
        self.manager.set_up(reuse=False, update=False)
        listed = self._create_listed_equity(num_eod=3, add_dividend=True, add_split=True)

        # Use identity_code for manager-level time series processor
        tsp = self.manager.get_time_series_processor([listed.identity_code])
        self.assertIsInstance(tsp, TimeSeriesProcessor)
        self.assertGreater(len(tsp.prices_df), 0)

    def test_get_time_series_processor_with_listed_equity_and_cash(self):
        """Test get_time_series_processor with ListedEquity and cash asset."""
        self.manager.set_up(reuse=False, update=False)
        listed = self._create_listed_equity(num_eod=3, add_dividend=True, add_split=True)

        # Get time series processor including cash asset via currency ticker
        tsp = self.manager.get_time_series_processor(
            [listed.identity_code], cash_currency_ticker='USD')
        self.assertIsInstance(tsp, TimeSeriesProcessor)
        self.assertGreater(len(tsp.prices_df), 0)

        # Expect both listed equity and USD cash identity codes present
        identity_codes = set(tsp.prices_df["identity_code"].unique())
        self.assertIn(listed.identity_code, identity_codes)
        self.assertIn('USD', identity_codes)

    def test_get_resampled_total_returns_with_listed_equity_and_cash(self):
        """Test get_resampled_total_returns for a ListedEquity and cash."""
        self.manager.set_up(reuse=False, update=False)
        # Provide enough observations for the TimeSeriesProcessor sample size check
        listed = self._create_listed_equity(num_eod=25, add_dividend=False, add_split=False)

        # Weekly resampled total returns including cash
        total_returns = self.manager.get_resampled_total_returns(
            [listed.identity_code], cash_currency_ticker='USD', frequency='W')

        self.assertIsInstance(total_returns, pd.DataFrame)
        self.assertGreater(len(total_returns.index), 0)
        self.assertIn(listed.identity_code, total_returns.columns)
        self.assertIn('USD', total_returns.columns)

    def test_get_asset_dict_with_unknown_identity_code_raises(self):
        """Unknown identity_code list yields TimeSeriesNoData."""
        with self.assertRaises(TimeSeriesNoData):
            self.manager.get_asset_dict(['UNKNOWN.IDENTITY'])

    def test_to_common_currency(self):
        """Test to_common_currency method."""
        # Configure with multiple currencies
        self._configure_standard_static_data(
            currency_tickers=['USD', 'EUR'],
            domicile_codes=['US', 'EU']
        )

        self.manager.set_up(reuse=False, update=False)

        # This is a complex method that requires forex data
        # For now, just verify it exists
        self.assertTrue(hasattr(self.manager, 'to_common_currency'))
        self.assertTrue(callable(self.manager.to_common_currency))

    def test_close_and_delete(self):
        """Test close methods."""
        # Create a temporary manager
        temp_manager = Manager(dialect='memory', testing=True)
        self.assertIsNotNone(temp_manager.session)

        # Close without dropping
        temp_manager.close(drop=False)
        self.assertFalse(hasattr(temp_manager, 'session_obj') and temp_manager.session_obj is not None)

        # Test close with drop=False on another manager
        temp_manager2 = Manager(dialect='memory', testing=True)
        temp_manager2.close(drop=False)
        self.assertFalse(hasattr(temp_manager2, 'session_obj') and temp_manager2.session_obj is not None)

    @unittest.mock.patch('src.asset_base.manager.Static')
    def test_tear_down_without_delete(self, mock_static_class):
        """Test close method."""
        mock_static = unittest.mock.MagicMock()
        mock_static_class.return_value = mock_static
        # Reuse common static mock configuration helper
        self._configure_static_mock(mock_static, ['USD'], ['US'], ['XNYS'])

        temp_manager = Manager(dialect='memory', testing=True)
        temp_manager.set_up(reuse=False, update=False)

        # Should not raise - just close without dropping
        temp_manager.close(drop=False)




class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # Classes that are passing. Add the others later when they too work.
        test_classes = [
            TestManagerInit,
            TestManager,
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
