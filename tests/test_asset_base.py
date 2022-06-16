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

import pandas as pd
from asset_base.asset import ListedEquity
from asset_base.exceptions import TimeSeriesNoData

from asset_base.financial_data import Dump

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from asset_base.asset_base import AssetBase, replace_time_series_labels
from asset_base.time_series import Dividend, TradeEOD
import asset_base.financial_data as fd

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


def assert_no_index_duplicates(security, security1, security2):
    """Assert no duplicate time series entries."""
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
            self.assertFalse(  # BUG:
                index.any(),
                f'The {series} series of {sec.identity_code} '
                'has duplicates in it\'s index.')


class TestSession(object):
    """Set up a test database and session."""

    def __init__(self):
        self.engine = create_engine('sqlite://', echo=True)
        Base.metadata.create_all(self.engine)  # Using asset_base.Base
        self.session = Session(self.engine)


class TestAssetBase(unittest.TestCase):
    """Set up and tear down the asset_base manager.

    This test is complex and different enough that it warrants it's own test.
    """

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = AssetBase

        # Make a memory based asset_base session with test data.
        # Set up with only AAPL, MCD and STX40 respectively
        cls.isin = 'US0378331005'
        cls.isin1 = 'US5801351017'
        cls.isin2 = 'ZAE000027108'
        cls.isin_list = [cls.isin, cls.isin1, cls.isin2]

        # Cash is USD
        cls.cash_ticker = 'USD'

    @classmethod
    def tearDownClass(cls):
        """ Tear down class test fixtures. """
        pass

    def setUp(self):
        """Set up test case fixtures."""
        self.asset_base = AssetBase(dialect='memory', testing=True)
        self.session = self.asset_base.session
        # For a fresh test delete any previously dumped data. Do not delete the
        # folder which was set up as a required test fixture.
        self.asset_base.dumper.delete(delete_folder=False)
        # Set-up the database with selected test securities
        self.asset_base.set_up(_test_isin_list=self.isin_list,)
        # This is also a test of AssetBase.set_up() and AssetBase.tear_down()

    def tearDown(self):
        """Tear down test case fixtures."""
        # Tear down asset_base and delete the dump folder and its contents so
        # it does not pollute other tests.
        self.asset_base.tear_down(delete_dump_data=True)
        # This is also a test of AssetBase.set_up() and AssetBase.tear_down()

    def test___init__(self):
        """Instance initialization."""
        self.assertIsInstance(self.asset_base, AssetBase)

    def test_dump(self):
        """Dump re-usable content to disk files."""
        self.asset_base.dump()
        # Must have dumped ListedEquity data for this test to work!
        dumper = Dump(testing=True)
        data = dumper.read(['ListedEquity', 'TradeEOD', 'Dividend'])
        self.assertIsInstance(data, dict)
        self.assertIn('ListedEquity', data.keys())
        self.assertIn('TradeEOD', data.keys())
        self.assertIn('Dividend', data.keys())

    def test_reuse(self):
        """Reuse dumped data as a database initialization resource."""
        # Dump the reusable part of the database contents for reuse.
        self.asset_base.dump()
        # Tear down asset_base and delete the dump folder, but not its contents,
        # so that we can initialise the database by reusing the dumped data
        self.asset_base.tear_down(delete_dump_data=False)
        # Set-up the database using only dumped data, do not update from feeds.
        self.asset_base.set_up(update=False, _test_isin_list=self.isin_list)
        # The old session was delete and a new one created.
        self.session = self.asset_base.session

        # Get database data
        securities_data = ListedEquity.to_data_frame(self.session)
        eod_data = TradeEOD.to_data_frame(self.session, ListedEquity)
        dividend_data = Dividend.to_data_frame(self.session, ListedEquity)
        # Get test data from feed
        fundamentals = fd.SecuritiesFundamentals()
        history = fd.SecuritiesHistory()
        securities_list = self.session.query(ListedEquity).all()
        securities_test_data = fundamentals.get_securities(_test_isin_list=self.isin_list)
        eod_test_data = history.get_eod(securities_list)
        dividend_test_data = history.get_dividends(securities_list)
        # Test - reused data should be same as feed data
        pd.testing.assert_frame_equal(
            securities_test_data[
                securities_data.columns].reset_index(drop=True),
            securities_data.reset_index(drop=True))
        pd.testing.assert_frame_equal(
            eod_test_data.drop(
                columns='adjusted_close').sort_index(axis=1).sort_values(
                    ['date_stamp', 'isin']).reset_index(drop=True),
            eod_data.drop(
                columns='adjusted_close').sort_index(axis=1).sort_values(
                    ['date_stamp', 'isin']).reset_index(drop=True))
        pd.testing.assert_frame_equal(
            dividend_test_data.drop(
                columns='adjusted_value').sort_index(axis=1).sort_values(
                    ['date_stamp', 'isin']).reset_index(drop=True),
            dividend_data.drop(
                columns='adjusted_value').sort_index(axis=1).sort_values(
                    ['date_stamp', 'isin']).reset_index(drop=True))

    def test_time_series(self):
        """Test all securities are functional using total returns checksum.

        """
        # Test total_return check-product over short historical date window.
        securities_list = self.session.query(ListedEquity).all()
        data = self.asset_base.time_series(
            securities_list, return_type='total_return')
        replace_time_series_labels(data, 'ticker', inplace=True)
        data = data.loc['2020-01-01':'2021-01-01']  # Fixed historical window
        data_check_prod = data.prod().to_dict()
        test_data = {
            'STX40': 1.0807429980281673,
            'AAPL': 0.4534256160141147,
            'MCD': 1.113204809354809}
        self.assertEqual(test_data, data_check_prod)


class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # Classes that are passing. Add the others later when they too work.
        test_classes = [
            TestSession,
            TestAssetBase,
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
