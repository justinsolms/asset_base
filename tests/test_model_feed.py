#!/usr/bin/env unittest
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Test suite for the model_feed module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the fundmanage module.
The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""

import unittest

from asset_base.model_feed import TestData, ModelFeed
from asset_base.entitybase import Model

from fundmanage.tests.fixture_entitybase import *
from fundmanage.tests.fixture_financial_feed import *


class TestTestData(unittest.TestCase):
    """Sunstrike Capital XML model test data fetch class."""

    @classmethod
    def setUpClass(self):
        """Set up test fixtures."""
        self.isin = ['ZA9876543214', 'ZA9876543222']

        fixture_entitybase(self)
        fixture_financial_feed(self)

    @classmethod
    def tearDownClass(self):
        """Tear down test fixtures."""
        self.entitybase.tear_down()

    def setUp(self):
        """Set up test case fixtures."""
        self.test_data = TestData(self.entitybase)

    def test___init__(self):
        """Initialization."""
        self.assertIsInstance(self.test_data, TestData)

    def test_fetch(self):
        self.test_data.fetch()
        session = self.entitybase.session
        model = Model.factory(session,
                              ticker='MODTICK1',
                              issuer_name='Ze Modeller',
                              issuer_domicile_code='ZA'
                              )
        self.assertEqual('%r' % model, '<Model:ZA:Ze Modeller:MODTICK1>')
        copy = Model.factory(session,
                             ticker='COPYTICK1',
                             issuer_name='Ze Ozzer Modeller',
                             issuer_domicile_code='ZA'
                             )
        self.assertEqual('%r' % copy, '<Model:ZA:Ze Ozzer Modeller:COPYTICK1>')


class TestModelFeed(unittest.TestCase):
    """Manage model data feeds and data."""

    @classmethod
    def setUpClass(self):
        """Set up test fixtures."""
        self.isin = ['ZA9876543214', 'ZA9876543222']
        self.model_tickers = ['MODTICK1', 'MODTIOCK2']

        fixture_entitybase(self)
        fixture_financial_feed(self)

    @classmethod
    def tearDownClass(self):
        """Tear down test fixtures."""
        self.entitybase.tear_down()

    def setUp(self):
        """Set up test case fixtures."""
        self.model_feed = ModelFeed(self.entitybase)

    def test_init(self):
        """Initialization."""
        self.assertIsInstance(self.model_feed, ModelFeed)

    def test_add_model(self):
        """Add models to the entitybase database."""
        # Add form test data
        self.model_feed.add_model('testdata')
        # Test retrieval
        session = self.entitybase.session
        parent = Model.factory(session,
                              ticker='MODTICK1',
                              issuer_name='Ze Modeller',
                              issuer_domicile_code='ZA'
                              )
        self.assertEqual('%r' % parent, '<Model.ZA.Ze Modeller.MODTICK1>')
        child = Model.factory(session,
                             ticker='COPYTICK1',
                             issuer_name='Ze Ozzer Modeller',
                             issuer_domicile_code='ZA'
                             )
        self.assertEqual('%r' % child, '<Model.ZA.Ze Ozzer Modeller.COPYTICK1>')
        # Test inherited parameters
        parent_parameters = parent.get_parameter_dict()
        child_parameters = child.get_parameter_dict()
        #  Check parent
        self.assertIn('cash_minimums_default_absolute', parent_parameters)
        self.assertEqual(parent_parameters['cash_minimums_default_absolute'], '5.0')
        # Check child overwrites
        self.assertIn('cash_minimums_default_absolute', child_parameters)
        self.assertEqual(child_parameters['cash_minimums_default_absolute'], '10.0')
        # Check child added something new
        self.assertIn('something_else', child_parameters)
        self.assertEqual(child_parameters['something_else'], '123')

class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # suite.addTest(TestTestData('test_init'))
        suite.addTest(TestTestData('test_fetch'))

        # suite.addTest(TestModelFeed('test_init'))
        suite.addTest(TestModelFeed('test_add_model'))

        self.suite = suite

    def run(self):
        runner=unittest.TextTestRunner()
        runner.run(self.suite)

if __name__ == '__main__':

    suite = Suite()
    suite.run()
