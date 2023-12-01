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
import pandas as pd

from sqlalchemy_utils import drop_database, database_exists

from src.asset_base.common import Common
from src.asset_base.financial_data import Dump
from src.asset_base.asset import Forex, ListedEquity
from src.asset_base.time_series import Dividend, ListedEOD
from src.asset_base.manager import Manager, replace_time_series_labels
from src.asset_base.exceptions import TimeSeriesNoData

import warnings

# Get module-named logger.
import logging

logger = logging.getLogger(__name__)


warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

# TODO: Test all __table_args__.UniqueConstraint attributes


def assert_no_index_duplicates(security, security1, security2):
    """Assert no duplicate time series entries."""
    security_list = [security, security1, security2]
    series_list = ["price", "dividend", "volume"]
    for sec in security_list:
        for series in series_list:
            try:
                data = security.time_series(series=series)
            except TimeSeriesNoData:
                pass  # No data, therefore no duplicates
            # Parameter `keep=False` actually means "keep duplicates"!!!
            index = data.index.duplicated(keep=False)
            self.assertFalse(  # BUG: We cannot use `self` here
                index.any(),
                f"The {series} series of {sec.identity_code} "
                "has duplicates in it's index.",
            )

class TestManagerInit(unittest.TestCase):
    """Manager sessions with different backend databases"""

    def tearDown(self):
        """Tear down test case fixtures."""
        # Delete database
        manager = self.manager
        if database_exists(manager.engine.url):
            manager.session.close()
            manager.engine.dispose()
            drop_database(manager.engine.url)
            # Delete specific attributes
            del manager.db_url
            del manager.engine
            del manager.session

    def common_todo(self):
        """Some common post creation tests using ``Common`` class."""
        test_name = 'Common class instance'
        session = self.manager.session
        test_common = Common(test_name)
        session.add(test_common)
        # FIXME: (sqlite3.OperationalError) no such table: common
        common = session.query(Common).one()
        self.assertEqual(test_common, common)
        self.assertEqual(common.name, test_name)

    def test_make_session_memory(self):
        """Make database sessions in either sqlite, mysql or memory."""
        self.manager = Manager(dialect='memory', testing=True)
        self.common_todo()

    def test_make_session_sqlite(self):
        """Make database sessions in either sqlite, mysql or memory."""
        self.manager = Manager(dialect='sqlite', testing=True)
        self.common_todo()


class TestManager(unittest.TestCase):
    """Set up and tear down the asset_base manager.

    This test is complex and different enough that it warrants it's own test.
    """

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Manager

        # Make a memory based asset_base session with test data.
        # Set up with only AAPL, MCD and STX40 respectively
        cls.isin = "US0378331005"
        cls.isin1 = "US5801351017"
        cls.isin2 = "ZAE000027108"
        cls.isin_list = [cls.isin, cls.isin1, cls.isin2]
        cls.foreign_currencies_list = ["USD", "EUR", "ZAR"]

        # Cash is USD
        cls.cash_ticker = "USD"

    @classmethod
    def tearDownClass(cls):
        """Tear down class test fixtures."""
        pass

    def setUp(self):
        """Set up test case fixtures."""
        self.manager = Manager(dialect="memory", testing=True)
        self.session = self.manager.session
        # For a fresh test delete any previously dumped data. Do not delete the
        # folder which was set up as a required test fixture.
        self.manager.dumper.delete(delete_folder=False)
        # Set-up the database with selected test securities
        self.manager.set_up(
            _test_isin_list=self.isin_list,
            _test_forex_list=self.foreign_currencies_list,
        )
        # This is also a test of Manager.set_up() and Manager.tear_down()

    def tearDown(self):
        """Tear down test case fixtures."""
        # Tear down asset_base and delete the dump folder and its contents so
        # it does not pollute other tests.
        self.manager.tear_down(delete_dump_data=True)
        # This is also a test of Manager.set_up() and Manager.tear_down()

    def test___init__(self):
        """Instance initialization."""
        self.assertIsInstance(self.manager, Manager)

    def test_dump(self):
        """Dump re-usable content to disk files."""
        self.manager.dump()
        # Must have dumped ListedEquity data for this test to work!
        dumper = Dump(testing=True)
        data = dumper.read(["ListedEquity", "ListedEOD", "Dividend"])
        self.assertIsInstance(data, dict)
        self.assertIn("ListedEquity", data.keys())
        self.assertIn("ListedEOD", data.keys())
        self.assertIn("Dividend", data.keys())

    def test_reuse(self):
        """Reuse dumped data as a database initialization resource."""
        # Get existing database data as test reference data
        securities_test_data = ListedEquity.to_data_frame(self.session)
        eod_test_data = ListedEOD.to_data_frame(self.session, ListedEquity)
        dividend_test_data = Dividend.to_data_frame(self.session, ListedEquity)
        # Tear down asset_base dumping data so that we can initialise the
        # database by reusing the dumped data
        self.manager.tear_down(delete_dump_data=False)
        # Set-up the database using only dumped data, do not update from API.
        self.manager.set_up(update=False, _test_isin_list=self.isin_list)
        # Get the newly created session
        self.session = self.manager.session

        # Get reused database data which was instantiated from the dumped data
        securities_data = ListedEquity.to_data_frame(self.session)
        eod_data = ListedEOD.to_data_frame(self.session, ListedEquity)
        dividend_data = Dividend.to_data_frame(self.session, ListedEquity)
        # Test - reused data should be same as feed data
        pd.testing.assert_frame_equal(
            securities_test_data[securities_data.columns].reset_index(drop=True),
            securities_data.reset_index(drop=True),
        )
        pd.testing.assert_frame_equal(
            eod_test_data.drop(columns="adjusted_close")
            .set_index(["date_stamp", "isin"])
            .sort_index(axis=0)  # Sorts index rank
            .sort_index(axis=1),  # Sorts column rank
            eod_data.drop(columns="adjusted_close")
            .set_index(["date_stamp", "isin"])
            .sort_index(axis=0)  # Sorts index rank
            .sort_index(axis=1),  # Sorts column rank
        )
        pd.testing.assert_frame_equal(
            dividend_test_data.drop(columns="adjusted_value")
            .set_index(["date_stamp", "isin"])
            .sort_index(axis=0)  # Sorts index rank
            .sort_index(axis=1),  # Sorts column rank
            dividend_data.drop(columns="adjusted_value")
            .set_index(["date_stamp", "isin"])
            .sort_index(axis=0)  # Sorts index rank
            .sort_index(axis=1),  # Sorts column rank
        )

    def test_time_series(self):
        """Test all securities time series."""
        # Test total-return product over short historical date window.
        securities_list = self.session.query(ListedEquity).all()
        data = self.manager.time_series(securities_list, return_type="total_return")
        replace_time_series_labels(data, "ticker", inplace=True)
        data = data.loc["2020-01-01":"2021-01-01"]  # Fixed historical window
        data_check_prod = data.prod().to_dict()
        test_data = {
            "STX40": 1.0807429980281673,
            "AAPL": 0.4557730329977179,
            "MCD": 1.113204809354809,
        }
        self.assertEqual(test_data, data_check_prod)

    def test_time_series_forex(self):
        """Test all securities time series with currency transformed to ZAR."""
        # Test total_return check-product over short historical date window.
        securities_list = self.session.query(ListedEquity).all()
        data = self.manager.time_series(securities_list, return_type="total_return")
        data_zar = self.manager.to_common_currency(data, "ZAR")
        data = replace_time_series_labels(data, "ticker")
        data_zar = replace_time_series_labels(data_zar, "ticker")
        forex = Forex.get_rates_data_frame(self.session, "ZAR", ["USD"])

        # Recover exchange rates. Correct recovery is the test that everything
        # works
        recover = data / data_zar
        recover = recover[["STX40", "AAPL", "MCD"]]
        # Extract values
        stx_40 = recover["STX40"][~recover["STX40"].isna()]
        aapl = recover["AAPL"][~recover["AAPL"].isna()]
        mcd = recover["MCD"][~recover["MCD"].isna()]
        # The MCD and APPL should, within rounding errors, correspond
        self.assertTrue(all(aapl.round(6) == mcd.round(6)))
        # This is ZAR to ZAR and should all be 1.0
        self.assertTrue(all(stx_40 == 1.0))
        # The ZARUSD exchange rate is properly recovered
        self.assertTrue(all(forex.reindex(aapl.index).USD.round(6) == aapl.round(6)))


class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # Classes that are passing. Add the others later when they too work.
        test_classes = [
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
