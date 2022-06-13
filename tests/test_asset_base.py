#!/usr/bin/env unittest
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Test suite for the asset_base module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>. This file is part of
the fundmanage module. The fundmanage module can not be modified, copied and/or
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
import sys
import datetime
import pandas as pd
import pandas

from asset_base.financial_data import Static
from asset_base.financial_data import SecuritiesFundamentals
from asset_base.financial_data import SecuritiesHistory

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import asset_base.asset_base as asset_base
from asset_base.exceptions import BadISIN, ReconcileError
from asset_base.time_series import TimeSeriesNoData
from asset_base.exceptions import FactoryError
from asset_base.asset import Currency
from asset_base.entity import Domicile
from asset_base.entity import Entity, EntityWeight
from asset_base.entity import Issuer, Exchange
from asset_base.asset import Cash
from asset_base.asset import Asset, Share
from asset_base.time_series import TimeSeriesBase, TradeEOD, Dividend
from asset_base.asset import Listed, ListedEquity
from asset_base.model import Model, ModelParameter
from asset_base.asset_base import EntityBase
from asset_base.asset_base import Base

from fundmanage.utils import date_to_str

# Get module-named logger.
import warnings
import logging
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Change logging level here.
# logger.setLevel(os.environ.get('LOG_LEVEL', logging.DEBUG))

# TODO: Test all __table_args__.UniqueConstraint attributes

warnings.filterwarnings(
    action="ignore", message="unclosed", category=ResourceWarning)


class TestSession(object):
    """Set up a test database and session."""

    def __init__(self):
        self.engine = create_engine('sqlite://', echo=True)
        Base.metadata.create_all(self.engine)  # Using asset_base.Base
        self.session = Session(self.engine)


class TestEntityBaseSetUp(unittest.TestCase):
    """Set up and tear down the asset_base manager.

    This test is complex and different enough that it warrants it's own test.
    """

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = EntityBase

        # Make a memory based asset_base session with test data.
        # Set up with only AAPL, MCD and STX40 respectively
        cls.isin = 'US0378331005'
        cls.isin1 = 'US5801351017'
        cls.isin2 = 'ZAE000027108'

        # Cash is USD
        cls.cash_ticker = 'USD'

    @classmethod
    def tearDownClass(cls):
        """ Tear down class test fixtures. """
        pass

    def setUp(self):
        """Set up test case fixtures."""
        self.asset_base = EntityBase(dialect='memory', testing=True)
        self.session = self.asset_base.session

        # For a fresh test delete any previously dumped data, but keep the dump
        # folder
        self.asset_base.delete_dumps(delete_folder=False)

        # Set-up the database with some securities
        self.asset_base.set_up(
            _test_isin_list=[self.isin, self.isin1, self.isin2],
            )

    def tearDown(self):
        """Tear down test case fixtures."""
        # Tear down asset_base and delete the dump folder and its contents.
        self.asset_base.tear_down(delete_dump_data=True)

    def test___init__(self):
        """Instance initialization."""
        self.assertIsInstance(self.asset_base, EntityBase)

    def test_set_up(self):
        """Set up the database."""
        # Assert no duplicate time series entries
        def assert_no_index_duplicates(security, security1, security2):
            security_list = [security, security1, security2]
            series_list = ['price', 'dividend', 'volume']
            for sec in security_list:
                for series in series_list:
                    try:
                        data = security.time_series(series=series)
                    except TimeSeriesNoData:
                        pass  # No data, therefore no duplicates
                    # Parameter `keep=False` actually means "keep duplicates"!!!
                    index = data.index.duplicated(keep=False)
                    self.assertFalse(
                        index.any(),
                        f'The {series} series of {sec.identity_code} '
                        'has duplicates in it\'s index.')

        # Test securities proper existence
        security = ListedEquity.factory(self.session, isin=self.isin)
        security1 = ListedEquity.factory(self.session, isin=self.isin1)
        security2 = ListedEquity.factory(self.session, isin=self.isin2)
        self.assertEqual(security.isin, self.isin)
        self.assertEqual(security1.isin, self.isin1)
        self.assertEqual(security2.isin, self.isin2)

        # Test for duplicates in the time series
        assert_no_index_duplicates(security, security1, security2)

        # Tear the database down. This should dump some data for reuse at
        # the next set_up
        self.asset_base.tear_down()

        # Re-set-up the database again with some securities. This should reuse
        # the dump data and have a much younger from_date in the feed API
        # fetches
        self.asset_base.set_up(
            _test_isin_list=[self.isin, self.isin1, self.isin2]
            )

        # The session will have been closed by the `set_up` method this
        # preventing any further ORM based lazy object attribute loading.
        # Therefore we need to call for new object form the new session created
        # in `set_up`.
        self.session = self.asset_base.session  # Get the fresh session

        # Test for duplicates in the time series
        assert_no_index_duplicates(security, security1, security2)

        # Pull series as a test
        security = ListedEquity.factory(self.session, isin=self.isin)
        security1 = ListedEquity.factory(self.session, isin=self.isin1)
        security2 = ListedEquity.factory(self.session, isin=self.isin2)
        series = pd.concat(
            [security.time_series(return_type='price'),
             security.time_series(return_type='total_price')],
            axis='columns',
            keys=['price', 'total_price'])
        series1 = pd.concat(
            [security1.time_series(return_type='price'),
             security1.time_series(return_type='total_price')],
            axis='columns',
            keys=['price', 'total_price'])
        series2 = pd.concat(
            [security2.time_series(return_type='price'),
             security2.time_series(return_type='total_price')],
            axis='columns',
            keys=['price', 'total_price'])
        # Test series values
        self.assertEqual(
            [154.51, 160.51980297544614],
            series.loc['2022-05-10'].to_list())
        self.assertEqual(
            [245.68, 439.6340131416558],
            series1.loc['2022-05-10'].to_list())
        self.assertEqual(
            [6110.0, 9278.878708810591],
            series2.loc['2022-05-10'].to_list())

        # Pull cash as a test
        cash_usd = Cash.factory(self.session, domicile_code='US')
        series_usd = cash_usd.time_series(date_index=series.index)
        series_aapl = security.time_series(return_type='price')
        series = pd.concat(
            [series_aapl, series_usd],
            axis='columns', keys=['price', 'cash'])
        # Test all cash prices == to 1.0
        self.assertTrue((series['cash'] == 1.0).all())


class TestEntityBase(unittest.TestCase):
    """The asset_base manager."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures.

        Note
        ----
        Contrary to earlier the test scheme TestEntityBaseSetUp, here we set up
        securities and their data once only as a permanent test fixture as we
        wish to test typical operational use-cases of a fully populated
        database. Testing set-up should have already happened in the class
        TestEntityBaseSetUp.
        """
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = EntityBase

        # Make a memory based asset_base session with test data.
        # Set up with only AAPL, MCD and STX40 respectively
        cls.isin = 'US0378331005'
        cls.isin1 = 'US5801351017'
        cls.isin2 = 'ZAE000027108'

        # Cash is USD
        cls.cash_ticker = 'USD'
        cls.cash_ticker1 = 'ZAR'

        cls.isin_list = [cls.isin, cls.isin1, cls.isin2]
        cash_ticker_list = [cls.cash_ticker, cls.cash_ticker1]

        # Set-up the database with some securities. (See Note above!).
        cls.asset_base = EntityBase(dialect='memory', testing=True)
        cls.session = cls.asset_base.session
        cls.asset_base.set_up(_test_isin_list=cls.isin_list)

        # Get financial asset entities and cash currency
        cls.security_list = cls.session.query(Listed).filter(Listed.isin.in_(cls.isin_list)).all()
        cls.cash_list = cls.session.query(Cash).filter(Cash.ticker.in_(cash_ticker_list)).all()
        cls.asset_list = cls.security_list + cls.cash_list

    @classmethod
    def tearDownClass(cls):
        """ Tear down class test fixtures. """
        # Delete test dump folder and its contents.
        # Tear down asset_base and delete the dump folder and its contents.
        cls.asset_base.tear_down(delete_dump_data=True)

    def setUp(self):
        """Set up test case fixtures."""
        pass

    def tearDown(self):
        """Tear down test case fixtures."""
        pass

    def test___init__(self):
        """Instance initialization."""
        self.assertIsInstance(self.asset_base, EntityBase)

    def test_time_series(self):
        """Cash and non-cash securities."""
        # Retrieve time series
        data = self.asset_base.time_series(self.asset_list)
        data_tickers = asset_base.replace_time_series_labels(data, 'ticker')
        # Test column labels
        self.assertAlmostEqual(
            [s.ticker for s in self.asset_list].sort(),
            data_tickers.columns.to_list().sort()),
        # Test content
        self.assertEqual(24.7520, data_tickers.loc['1984-11-05']['AAPL'])
        self.assertEqual(1.0, data_tickers.loc['1984-11-05']['USD'])
        # Test that data is up to date by comparing to last date EOD data.
        last_date_data = SecuritiesHistory().get_eod(self.security_list)
        last_date = last_date_data['date_stamp'].max()
        self.assertEqual(last_date, data.index[-1])
        # Test column labels
        data_id_code = asset_base.replace_time_series_labels(
            data, 'identity_code')
        columns_data = [s.identity_code for s in self.asset_list]
        columns_id_code = [s for s in data_id_code.columns]
        # FIXME: Test has weird currency ticker `PW.USD`. WTF!!
        # ['ZAE000027108.STX40', 'US0378331005.AAPL', 'US5801351017.MCD', 'PW.USD', 'ZA.ZAR']
        import ipdb; ipdb.set_trace()


class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # Classes that are passing. Add the others later when they too work.
        test_classes = [
            TestSession,
            TestEntityBaseSetUp,
            TestEntityBase,
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
