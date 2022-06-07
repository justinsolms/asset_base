#!/usr/bin/env unittest
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Test suite for the entitybase module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>. This file is part of
the fundmanage module. The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

The classmethod ``setUpClass`` sets up test class fixtures and method ``setUp``
sets up test case fixtures. The design of all the tests is to have ``setUp``
initialize a new and empty memory database for every test case.

All non-committed class fixtures are set up in the classmethod ``setUpClass``
and the committed ``entitybase`` ORM class instances are committed in the test
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

import asset_base.asset_base as entitybase
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
        Base.metadata.create_all(self.engine)  # Using entitybase.Base
        self.session = Session(self.engine)


class TestCurrency(unittest.TestCase):
    """Test the Currency class."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Currency
        # Currency data
        cls.get_method = Static().get_currency
        cls.currency_dataframe = cls.get_method()
        # A single currency
        cls.currency_item = cls.currency_dataframe[
            cls.currency_dataframe.currency_code == 'USD']
        cls.code = cls.currency_item.currency_code.to_list()[0]
        cls.name = cls.currency_item.currency_name.to_list()[0]
        # A second single currency
        cls.currency_item1 = cls.currency_dataframe[
            cls.currency_dataframe.currency_code == 'GBP']
        cls.code1 = cls.currency_item1.currency_code.to_list()[0]
        cls.name1 = cls.currency_item1.currency_name.to_list()[0]

    def setUp(self):
        """Set up test case fixtures."""

        # Each test with a clean entitybase
        self.entitybase = EntityBase(dialect='memory')
        self.session = self.entitybase.session

    def test___init__(self):
        """Initialization."""
        obj = Currency(code=self.code, name=self.name)
        self.assertIsInstance(obj, Currency)
        self.assertEqual(obj.code, self.code)
        self.assertEqual(obj.name, self.name)

    def test___str__(self):
        """String output."""
        obj = Currency(code=self.code, name=self.name)
        self.assertEqual(obj.__str__(), 'Currency is U.S. Dollar (USD)')

    def test_key_code(self):
        obj = Currency(code=self.code, name=self.name)
        self.assertEqual(obj.key_code, 'USD')

    def test_identity_code(self):
        obj = Currency(code=self.code, name=self.name)
        self.assertEqual(obj.identity_code, 'USD')

    def test_factory(self):
        """Factory create."""
        # Despite using factory twice there should be only one instance
        obj = Currency.factory(self.session, self.code, self.name)
        obj = Currency.factory(self.session, self.code, self.name)
        self.assertEqual(len(self.session.query(Currency).all()), 1)
        self.assertEqual(obj.code, self.code)
        self.assertEqual(obj.name, self.name)

    def test_factory_fail_create(self):
        """Fail create new with no currency name provided."""
        with self.assertRaises(FactoryError):
            Currency.factory(self.session, self.code)

    def test_factory_change(self):
        """Currency name changed."""
        obj = Currency.factory(self.session, self.code, self.name)
        new_name = 'A Changed Currency Name for Testing'
        obj = Currency.factory(self.session, self.code, new_name)
        self.assertEqual(obj.name, new_name)

    def test_factory_fail(self):
        """Instance Factory Fails."""
        with self.assertRaises(FactoryError):
            # Non-existent instance need currency_name argument to create
            Currency.factory(self.session, self.code)

    def test_from_data_frame(self):
        """Get data from a pandas.DataFrame."""
        Currency.from_data_frame(self.session, self.currency_dataframe)
        # Test a currency
        obj = Currency.factory(self.session, self.code)
        self.assertEqual(obj.code, self.code)
        self.assertEqual(obj.name, self.name)
        # Test a a second currency
        obj = Currency.factory(self.session, self.code1)
        self.assertEqual(obj.code, self.code1)
        self.assertEqual(obj.name, self.name1)

    def test_update_all(self):
        """Create/update all Currency objects from the financial_data module"""
        Currency.update_all(self.session, self.get_method)
        # Test a currency
        obj = Currency.factory(self.session, self.code)
        self.assertEqual(obj.code, self.code)
        self.assertEqual(obj.name, self.name)
        # Test a a second currency
        obj = Currency.factory(self.session, self.code1)
        self.assertEqual(obj.code, self.code1)
        self.assertEqual(obj.name, self.name1)


class TestDomicile(unittest.TestCase):
    """Domicile and related Currency

    Note
    ----
    All Currency object instances shall be in entitybase before any dependent
    Domicile instances are created.
    """

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Domicile
        # Domicile data
        cls.get_method = Static().get_domicile
        cls.domicile_dataframe = cls.get_method()
        # A single domicile with currency
        cls.domicile_item = cls.domicile_dataframe[
            cls.domicile_dataframe.domicile_code == 'US']
        cls.domicile_code = cls.domicile_item.domicile_code.to_list()[0]
        cls.domicile_name = cls.domicile_item.domicile_name.to_list()[0]
        cls.currency_code = cls.domicile_item.currency_code.to_list()[0]
        # A second single domicile with currency
        cls.domicile_item1 = cls.domicile_dataframe[
            cls.domicile_dataframe.domicile_code == 'GB']
        cls.domicile_code1 = cls.domicile_item1.domicile_code.to_list()[0]
        cls.domicile_name1 = cls.domicile_item1.domicile_name.to_list()[0]
        cls.currency_code1 = cls.domicile_item1.currency_code.to_list()[0]

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean entitybase
        self.entitybase = EntityBase(dialect='memory')
        self.session = self.entitybase.session
        # Add all currency objects to entitybase
        Currency.update_all(self.session, get_method=Static().get_currency)
        self.currency = Currency.factory(self.session, self.currency_code)
        self.currency1 = Currency.factory(self.session, self.currency_code1)

    def test___init__(self):
        domicile = Domicile(self.domicile_code,
                            self.domicile_name, self.currency)
        self.assertIsInstance(domicile, Domicile)
        self.assertEqual(domicile.code, self.domicile_code)
        self.assertEqual(domicile.name, self.domicile_name)
        self.assertEqual(domicile.currency, self.currency)

    def test_key_code(self):
        domicile = Domicile(self.domicile_code,
                            self.domicile_name, self.currency)
        self.assertEqual(domicile.key_code, 'US')

    def test_identity_code(self):
        domicile = Domicile(self.domicile_code,
                            self.domicile_name, self.currency)
        self.assertEqual(domicile.identity_code, 'US')

    def test___str__(self):
        domicile = Domicile(self.domicile_code,
                            self.domicile_name, self.currency)
        self.assertEqual(domicile.__str__(), 'Domicile is United States (US)')

    def test_factory(self):
        """Instance Factory."""
        # Add twice, should retrieve one.
        domicile = Domicile.factory(self.session, self.domicile_code,
                                    self.domicile_name, self.currency.code)
        domicile = Domicile.factory(self.session, self.domicile_code,
                                    self.domicile_name, self.currency.code)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Domicile).all()), 1)
        self.assertEqual(domicile.code, self.domicile_code)
        self.assertEqual(domicile.name, self.domicile_name)
        self.assertEqual(domicile.currency, self.currency)

    def test_factory_change(self):
        """Instance Factory handles changes."""
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency.code)
        # Change domicile name. Change currency
        new_domicile_name = 'A New Domicile Name for Testing'
        new_currency_code = self.currency1.code
        domicile = Domicile.factory(self.session, self.domicile_code,
                                    new_domicile_name, new_currency_code)
        self.assertEqual(domicile.name, new_domicile_name)
        # Bad currency code
        with self.assertRaises(FactoryError):
            # Point to non-existent currency
            Domicile.factory(
                self.session, self.domicile_code, self.domicile_name,
                'INVALID_CURRENCY_CODE')

    def test_factory_fail(self):
        """Instance Factory Fails."""
        with self.assertRaises(FactoryError):
            # Non-existent instance need full arguments argument to create
            Domicile.factory(self.session, self.domicile_code)

    def test_from_data_frame(self):
        """Get data from a pandas.DataFrame."""
        Domicile.from_data_frame(
            self.session, data_frame=self.domicile_dataframe)
        # Test a domicile
        obj = Domicile.factory(self.session, self.domicile_code)
        self.assertEqual(obj.code, self.domicile_code)
        self.assertEqual(obj.name, self.domicile_name)
        # Test a a second domicile
        obj = Domicile.factory(self.session, self.domicile_code1)
        self.assertEqual(obj.code, self.domicile_code1)
        self.assertEqual(obj.name, self.domicile_name1)

    def test_update_all(self):
        """Create/update all Domicile objects from the financial_data module"""
        Domicile.update_all(self.session, self.get_method)
        # Test a currency
        obj = Domicile.factory(self.session, self.domicile_code)
        self.assertEqual(obj.code, self.domicile_code)
        self.assertEqual(obj.name, self.domicile_name)
        # Test a a second currency
        obj = Domicile.factory(self.session, self.domicile_code1)
        self.assertEqual(obj.code, self.domicile_code1)
        self.assertEqual(obj.name, self.domicile_name1)


class TestEntityWeight(unittest.TestCase):
    """The weight of a single held child entity."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = EntityWeight
        # Test strings
        cls.currency_code = 'ZZZ'
        cls.currency_name = 'The Currency'
        cls.domicile_code = 'ZZ'
        cls.domicile_name = 'The Place'
        cls.name_parent = 'The Parent Entity'
        cls.name_child1 = 'The Child Entity 1'
        cls.name_child2 = 'The Child Entity 2'
        cls.name_child3 = 'The Child Entity 3'
        cls.name_child4 = 'The Child Entity 4'
        cls.name_child5 = 'The Child Entity 5'
        cls.name_child6 = 'The Child Entity 6'
        cls.weight1 = 0.8
        cls.weight2 = 0.2
        cls.weight3 = 0.6
        cls.weight4 = 0.4
        cls.weight5 = 0.6
        cls.weight6 = 0.4
        cls.test_str = 'The Parent Entity is an entity in The Place.'

    def setUp(self):
        """Set up test case fixtures."""
        self.session = TestSession().session

    def test___init__(self):
        """Instance initialization."""
        currency = Currency(self.currency_code, self.currency_name)
        domicile = Domicile(self.domicile_code, self.domicile_name,
                            currency)
        #  Create parent.
        parent = Entity(self.name_parent, domicile)
        # Create child entities.
        child1 = Entity(self.name_child1, domicile)
        # Create EntityWeight instances.
        entity_weight1 = EntityWeight(parent, child1, self.weight1)
        # Create parent entity.
        self.assertIsInstance(entity_weight1, EntityWeight)

    def test___str__(self):
        """Strings"""
        currency = Currency(self.currency_code, self.currency_name)
        domicile = Domicile(self.domicile_code, self.domicile_name,
                            currency)
        #  Create parent.
        parent = Entity(self.name_parent, domicile)
        # Create child entities.
        child1 = Entity(self.name_child1, domicile)
        # Create EntityWeight instances.
        entity_weight1 = EntityWeight(parent, child1, self.weight1)
        # Strings.
        self.assertEqual(
            entity_weight1.__str__(),
            '(The Parent Entity is an entity in The Place.)-holds(0.8000)->\
                (The Child Entity 1 is an entity in The Place.)'
        )

    def test_factory(self):
        """Factory adds children weights."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 2)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Entity.factory(self.session, self.name_parent, self.domicile_code,
                       children=children_weights)
        self.session.commit()
        #  Retrieve and reconcile.
        obj = Entity.factory(self.session, self.name_parent, self.domicile_code)
        self.assertEqual(self.test_str, obj.__str__())

    def test_factory_update(self):
        """Factory adds children weights then updates them."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        en3 = Entity.factory(self.session, self.name_child3, self.domicile_code)
        en4 = Entity.factory(self.session, self.name_child4, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 4)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Entity.factory(self.session, self.name_parent, self.domicile_code,
                       children=children_weights)
        self.session.commit()
        #  Retrieve and reconcile.
        obj = Entity.factory(self.session, self.name_parent, self.domicile_code)
        # Strings.
        self.assertEqual(
            obj.children_weights[0].__str__(),
            '(The Parent Entity is an entity in The Place.)-holds(0.8000)->\
                (The Child Entity 1 is an entity in The Place.)')
        self.assertEqual(
            obj.children_weights[1].__str__(),
            '(The Parent Entity is an entity in The Place.)-holds(0.2000)->\
                (The Child Entity 2 is an entity in The Place.)')
        # Changed children weight dictionary.
        children_weights = {
            en3.id: self.weight3,
            en4.id: self.weight4,
        }
        #  Retrieve and update.
        obj = Entity.factory(self.session, self.name_parent, self.domicile_code,
                             children=children_weights)
        self.session.commit()
        # Strings.
        self.assertEqual(
            obj.children_weights[0].__str__(),
            '(The Parent Entity is an entity in The Place.)-holds(0.6000)->\
                (The Child Entity 3 is an entity in The Place.)')
        self.assertEqual(
            obj.children_weights[1].__str__(),
            '(The Parent Entity is an entity in The Place.)-holds(0.4000)->\
                (The Child Entity 4 is an entity in The Place.)')

    def test_weights(self):
        """Compute weights of all entities in a holding. Use factory method.

        Create holding tree:

                      Parent vertice
                    /   \    Weighted edges.
          Child    1     2   Internal vertices.
                  / \   / \  Weighted edges.
          Child  3  4  5  6  Leaf vertices
        """
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add leaf children.
        en3 = Entity.factory(self.session, self.name_child3, self.domicile_code)
        en4 = Entity.factory(self.session, self.name_child4, self.domicile_code)
        en5 = Entity.factory(self.session, self.name_child5, self.domicile_code)
        en6 = Entity.factory(self.session, self.name_child6, self.domicile_code)
        # Child 1.
        children_weights1 = {
            en3.id: self.weight3,
            en4.id: self.weight4,
        }
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code,
                             children=children_weights1)
        # Child 2.
        children_weights2 = {
            en5.id: self.weight5,
            en6.id: self.weight6,
        }
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code,
                             children=children_weights2)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add parent.
        Entity.factory(self.session, self.name_parent, self.domicile_code,
                       children=children_weights)
        self.session.commit()
        # Test
        answer = {
            'The Parent Entity': 1.00,
            'The Child Entity 1': 0.80,
            'The Child Entity 2': 0.20,
            'The Child Entity 3': 0.48,
            'The Child Entity 4': 0.32,
            'The Child Entity 5': 0.12,
            'The Child Entity 6': 0.08,
        }
        #  Get parent.
        parent = Entity.factory(self.session, self.name_parent,
                                self.domicile_code)
        #  All.
        result = parent.get_weights(which='all')
        for (entity, weight) in result:
            name = entity.name
            self.assertAlmostEqual(answer[name], weight, 15)
        # Leaves.
        result = parent.get_weights(which='leaves')
        for (entity, weight) in result:
            name = entity.name
            self.assertAlmostEqual(answer[name], weight, 15)


class TestEntity(unittest.TestCase):
    """The base class for all entities."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Entity
        # Domicile data
        cls.get_method = Static().get_domicile
        cls.domicile_dataframe = cls.get_method()
        # A single domicile with currency
        cls.domicile_item = cls.domicile_dataframe[
            cls.domicile_dataframe.domicile_code == 'US']
        cls.domicile_code = cls.domicile_item.domicile_code.to_list()[0]
        cls.domicile_name = cls.domicile_item.domicile_name.to_list()[0]
        cls.currency_code = cls.domicile_item.currency_code.to_list()[0]
        cls.name = 'Test Entity'
        cls.test_str = 'Test Entity is an entity in United States'
        cls.key_code = 'US.Test Entity'
        cls.identity_code = 'US.Test Entity'

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean entitybase
        self.entitybase = EntityBase(dialect='memory')
        self.session = self.entitybase.session
        # Add all Currency objects to entitybase
        Currency.update_all(self.session, get_method=Static().get_currency)
        # Add all Domicile objects to the entitybase
        Domicile.update_all(self.session, get_method=Static().get_domicile)
        self.domicile = Domicile.factory(self.session, self.domicile_code)

    def test___init__(self):
        entity = Entity(self.name, self.domicile)
        self.assertIsInstance(entity, Entity)
        # Attributes
        self.assertEqual(entity.name, self.name)
        self.assertEqual(entity.domicile.code, self.domicile.code)

    def test___str__(self):
        entity = Entity(self.name, self.domicile)
        self.assertEqual(
            entity.__str__(), self.test_str)

    def test_key_code(self):
        entity = Entity(self.name, self.domicile)
        self.assertEqual(entity.key_code, self.key_code)

    def test_identity_code(self):
        entity = Entity(self.name, self.domicile)
        self.assertEqual(entity.identity_code, self.identity_code)

    def test_factory(self):
        """Test session add entity but domicile and currency already added."""
        # Pre-add currency.
        # Add.
        entity = Entity.factory(self.session, self.name, self.domicile_code)
        entity = Entity.factory(self.session, self.name, self.domicile_code)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Entity).all()), 1)
        # Attributes
        self.assertEqual(entity.name, self.name)
        self.assertEqual(entity.domicile.code, self.domicile.code)
        # Get
        entity1 = Entity.factory(self.session, self.name, self.domicile_code)
        self.assertEqual(entity, entity1)

    def test_factory_fail(self):
        """Test session add fail if second add has wrong domicile_name."""
        with self.assertRaises(FactoryError):
            Entity.factory(self.session, self.name, 'WRONG_DOMICILE_CODE')

    def test_factory_no_create(self):
        """Test create parameter."""
        # Add.
        with self.assertRaises(FactoryError):
            Entity.factory(self.session, self.name, self.domicile_code,
                           create=False)

    def test_update_all(self):
        """Update all data form a getter method."""
        assert True  # FIXME: We don't test this yet.

    def test_key_code_id_table(self):
        """A table of all instance's ``Entity.id`` against ``key_code``."""
        Entity.factory(self.session, self.name, self.domicile_code)
        instances_list = self.session.query(Entity).all()
        test_df = pd.DataFrame(
            [(item.id, item.key_code) for item in instances_list],
            columns=['entity_id', 'key_code'])
        df = Entity.key_code_id_table(self.session)
        pd.testing.assert_frame_equal(test_df, df)


class TestInstitution(TestEntity):
    """No test needed due to trivial inheritance."""
    pass


class TestIssuer(TestEntity):
    """No test needed due to trivial inheritance."""
    pass


class TestExchange(TestInstitution):
    """
    Note
    ----
    Test inheritance forces all parent tests for the tested class attributes and
    methods to be reused or overridden or fail.

    """

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Exchange
        # Exchange data
        cls.get_method = Static().get_exchange
        cls.exchange_dataframe = cls.get_method()
        # A single exchange with currency
        cls.exchange_item = cls.exchange_dataframe[
            cls.exchange_dataframe.mic == 'XNYS']
        cls.mic = cls.exchange_item.mic.to_list()[0]
        cls.exchange_name = cls.exchange_item.exchange_name.to_list()[0]
        cls.domicile_code = cls.exchange_item.domicile_code.to_list()[0]
        cls.eod_code = cls.exchange_item.eod_code.to_list()[0]
        cls.test_str = 'USA Stocks is an exchange in United States'

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()

    def test___init__(self):
        exchange = Exchange(
            self.exchange_name, self.domicile, self.mic, eod_code=self.eod_code)
        self.assertIsInstance(exchange, Exchange)
        # Attributes
        self.assertEqual(exchange.name, self.exchange_name)
        self.assertEqual(exchange.domicile.code, self.domicile.code)
        self.assertEqual(exchange.mic, self.mic)
        self.assertEqual(exchange.eod_code, self.eod_code)

    def test___str__(self):
        exchange = Exchange(
            self.exchange_name, self.domicile, self.mic, eod_code=self.eod_code)
        self.assertEqual(
            exchange.__str__(),
            self.test_str)

    def test_key_code(self):
        exchange = Exchange(
            self.exchange_name, self.domicile, self.mic, eod_code=self.eod_code)
        self.assertEqual(exchange.key_code, 'XNYS')

    def test_identity_code(self):
        exchange = Exchange(
            self.exchange_name, self.domicile, self.mic, eod_code=self.eod_code)
        self.assertEqual(exchange.identity_code, 'XNYS')

    def test_factory(self):
        """Instance Factory."""
        # Add.
        exchange = Exchange.factory(
            self.session, self.mic, self.exchange_name, self.domicile_code,
            eod_code=self.eod_code)
        exchange = Exchange.factory(
            self.session, self.mic, self.exchange_name, self.domicile_code,
            eod_code=self.eod_code)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Exchange).all()), 1)
        # Attributes
        self.assertEqual(exchange.name, self.exchange_name)
        self.assertEqual(exchange.domicile.code, self.domicile.code)
        self.assertEqual(exchange.mic, self.mic)
        self.assertEqual(exchange.eod_code, self.eod_code)
        # Get is same
        exchange1 = Exchange.factory(self.session, mic=self.mic)
        self.assertEqual(exchange, exchange1)

    def test_factory_change(self):
        """Instance Factory handles changes."""
        # Add.
        Exchange.factory(
            self.session, self.mic, self.exchange_name, self.domicile_code,
            eod_code=self.eod_code)
        # Changes
        with self.assertRaises(ReconcileError):
            Exchange.factory(
                self.session, self.mic, exchange_name='newname')
        with self.assertRaises(ReconcileError):
            Exchange.factory(
                self.session, self.mic, domicile_code='newcode')

    def test_factory_fail(self):
        """Test session add fail if second add has wrong domicile_name."""
        with self.assertRaises(FactoryError):
            Exchange.factory(
                self.session, self.mic, self.exchange_name, 'WRONG_DOMICILE',
                eod_code=self.eod_code)

    def test_from_data_frame(self):
        """Get data from a pandas.DataFrame."""
        Exchange.from_data_frame(
            self.session, data_frame=self.exchange_dataframe)
        exchange = Exchange.factory(self.session, self.mic)
        # Attributes
        self.assertEqual(exchange.name, self.exchange_name)
        self.assertEqual(exchange.domicile.code, self.domicile.code)
        self.assertEqual(exchange.mic, self.mic)
        self.assertEqual(exchange.eod_code, self.eod_code)

    def test_update_all(self):
        """Create/update all Domicile objects from the financial_data module"""
        Exchange.update_all(self.session, self.get_method)
        exchange = Exchange.factory(self.session, self.mic)
        # Attributes
        self.assertEqual(exchange.name, self.exchange_name)
        self.assertEqual(exchange.domicile.code, self.domicile.code)
        self.assertEqual(exchange.mic, self.mic)
        self.assertEqual(exchange.eod_code, self.eod_code)

    def test_factory_no_create(self):
        """Test create parameter."""
        # Add.
        with self.assertRaises(FactoryError):
            Exchange.factory(
                self.session, self.mic, self.exchange_name, self.domicile_code,
                eod_code=self.eod_code, create=False)


class TestAsset(TestEntity):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        super().setUpClass()
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Asset
        # Test strings
        cls.name = 'Test Asset'
        cls.test_str = 'Test Asset is an entity in United States'
        cls.key_code = 'US.Test Asset'
        cls.identity_code = 'US.Test Asset'

    def test___init__(self):
        asset = Asset(self.name, self.domicile)
        self.assertIsInstance(asset, Asset)

    def test_factory(self):
        """Test session add asset but domicile and currency already added."""
        # FIXME: Drop test. We needed it only for a bug we had.
        # Pre-add currency.
        # Add.
        asset = Asset.factory(self.session, self.name, self.domicile_code)
        asset = Asset.factory(self.session, self.name, self.domicile_code)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Asset).all()), 1)
        # Attributes
        self.assertEqual(asset.name, self.name)
        self.assertEqual(asset.domicile.code, self.domicile.code)
        # Get
        asset1 = Asset.factory(self.session, self.name, self.domicile_code)
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
        Cash.factory(self.session, self.domicile_code)
        # Retrieve.
        cash = Cash.factory(self.session, self.domicile_code)
        # Test
        self.assertIsInstance(cash, Cash)
        self.assertEqual(cash.domicile.code, self.domicile_code)
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
        Cash.factory(self.session, self.domicile_code)
        # Retrieve.
        cash = Cash.factory(self.session, self.domicile_code)
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
        # Test strings
        cls.name = 'Test Share'
        cls.domicile_code = 'US'
        cls.issuer_name = 'The Issuer'
        cls.issuer_domicile_code = 'US'
        cls.key_code = 'US.The Issuer.Test Share'
        cls.identity_code = 'US.The Issuer.Test Share'
        cls.quote_units = 'cents'

    def test___init__(self):
        domicile = Domicile.factory(self.session, self.domicile.code)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        share = Share(self.name, issuer, domicile, quote_units=self.quote_units)
        self.assertIsInstance(share, Share)
        self.assertEqual(share.domicile, domicile)
        self.assertEqual(share.name, self.name)
        self.assertEqual(share.issuer, issuer)
        self.assertEqual(share.quote_units, self.quote_units)

    def test___str__(self):
        domicile = Domicile.factory(self.session, self.domicile.code)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        share = Share(self.name, issuer, domicile)
        self.assertEqual(
            share.__str__(),
            'Test Share is a share in United States \
                issued by The Issuer in United States')

    def test_key_code(self):
        domicile = Domicile.factory(self.session, self.domicile.code)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        share = Share(self.name, issuer, domicile)
        self.assertEqual(share.key_code, self.key_code)

    def test_identity_code(self):
        domicile = Domicile.factory(self.session, self.domicile.code)
        issuer = Issuer.factory(
            self.session, self.issuer_name, self.issuer_domicile_code)
        share = Share(self.name, issuer, domicile)
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
        cls.get_meta_method = SecuritiesFundamentals().get_securities
        cls.securities_dataframe = cls.get_meta_method()
        # Securities EOD-data
        cls.get_eod_method = SecuritiesHistory().get_eod
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
        # Test TradeEOD data
        cls.from_date = '2020-01-01'
        cls.to_date = '2020-12-31'
        cls.columns = [
            'date_stamp', 'ticker', 'mic', 'isin',
            'close', 'high', 'low', 'open', 'volume']
        cls.test_columns = [
            'close', 'high', 'low', 'open', 'volume']
        # Exclude adjusted_close as it varies
        # NOTE: These values may change as EOD historical data gets corrected
        cls.test_values = pd.DataFrame([  # Last date data
            [132.69, 134.74, 131.72, 134.08, 99116586.0],
            [214.58, 214.93, 210.78, 211.25, 2610914.0],
            [5460.0, 5511.0, 5403.0, 5492.0, 112700.0]
        ], columns=cls.test_columns)

        # Do not create Issuer instance here as it should be created during
        # testing only as later the Listed.factory method will create the issuer
        # from it's arguments.

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Add all Exchange objects to entitybase
        Exchange.update_all(self.session, get_method=Static().get_exchange)
        self.exchange = Exchange.factory(self.session, mic=self.mic)
        self.exchange1 = Exchange.factory(self.session, mic=self.mic1)
        self.exchange2 = Exchange.factory(self.session, mic=self.mic2)

    def to_eod_dict(self, item):
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
        self.assertEqual(listed.domicile.code, self.isin[0:2])
        self.assertEqual(listed.domicile.code, self.issuer_domicile_code)
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
        self.assertEqual(listed.domicile.code, self.isin[0:2])
        self.assertEqual(listed.domicile.code, self.issuer_domicile_code)
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

    def test_factory_fail_too_few_args(self):
        """Fail with too few arguments."""
        # Add. Issuer should be automatically created.
        Listed.factory(
            self.session,
            isin=self.isin, mic=self.mic, ticker=self.ticker,
            listed_name=self.name,
            issuer_domicile_code=self.issuer_domicile_code,
            issuer_name=self.issuer_name)
        # Test retrieval on ISIN fails Issuer requirement
        with self.assertRaises(FactoryError) as fail:
            Listed.factory(
                self.session,
                self.isin1)  # Wrong ISIN
        # Check by message content that we got the right exception
        self.assertIn('issuer', fail.exception.message)
        # Test retrieval on MIC, Ticker pair fails Issuer requirement
        with self.assertRaises(FactoryError) as fail:
            Listed.factory(
                self.session,
                ticker=self.ticker1,  # Wrong ticker
                mic=self.mic)
        # Check by message content that we got the right exception
        self.assertIn('issuer', fail.exception.message)
        # Test retrieval on MIC, Ticker pair fails Listed.__init__ argument
        # requirements
        with self.assertRaises(FactoryError) as fail:
            Listed.factory(
                self.session,
                issuer_domicile_code=self.issuer_domicile_code,
                issuer_name=self.issuer_name,
                ticker=self.ticker1,
                mic=self.mic)
        # Check by message content that we got the right exception
        self.assertIn('positional', fail.exception.message)
        # Test retrieval on MIC, Ticker pair fails Exchange.mic argument
        with self.assertRaises(FactoryError) as fail:
            Listed.factory(
                self.session,
                issuer_domicile_code=self.issuer_domicile_code,
                issuer_name=self.issuer_name,
                ticker=self.ticker,
                mic='BAD_MIC')
        # Check by message content that we got the right exception
        self.assertIn('exchange', fail.exception.message)

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
        self.assertEqual(listed.domicile.code, self.isin[0:2])
        self.assertEqual(listed.domicile.code, self.issuer_domicile_code)
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
        self.assertEqual(listed.domicile.code, self.isin[0:2])
        self.assertEqual(listed.domicile.code, self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)

    def test_key_code_id_table(self):
        """A table of all instance's ``Entity.id`` against ``key_code``."""
        # Insert all securities meta-data (for all securities)
        Listed.update_all(self.session, self.get_meta_method)
        instances_list = self.session.query(Listed).all()
        test_df = pd.DataFrame(
            [(item.id, item.key_code) for item in instances_list],
            columns=['entity_id', 'isin'])
        df = Listed.key_code_id_table(self.session)
        pd.testing.assert_frame_equal(test_df, df)

    def test_update_all_trade_eod(self):
        """Update all Listed and TradeEOD objs from their getter methods."""
        # Insert only selected subset of securities meta-data
        # Update all data instances: Listed & TradeEOD. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        Listed.update_all(  # Method to be tested
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            _test_isin_list=[self.isin, self.isin1, self.isin2])
        # Retrieve the submitted TradeEOD data from entitybase
        df = pd.DataFrame([self.to_eod_dict(item)
                           for item in self.session.query(TradeEOD).all()])
        # Test
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.sort_values(['date_stamp', 'ticker'], inplace=True)
        # Test against last date test_values data
        last_date = datetime.datetime.strptime(self.to_date, '%Y-%m-%d')
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)
        # Test security `time_series_last_date` attributes
        ts_last_date = TradeEOD.assert_last_dates(self.session)
        self.assertEqual(ts_last_date, datetime.date.today())

    def test_get_eod_trade_series(self):
        """Return the EOD trade pd.DataFrame series for the security."""
        # Insert only selected subset of Listed instances from meta-data
        # Listed.from_data_frame(
        #     self.session, data_frame=self.selected_securities_dataframe)
        # Update all data instances: Listed & TradeEOD. Force a limited set of 3
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
        df = listed.get_eod_trade_series()
        df1 = listed1.get_eod_trade_series()
        df2 = listed2.get_eod_trade_series()
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
        last_date = datetime.datetime.strptime(self.to_date, '%Y-%m-%d')
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)

    def test_get_last_eod_trades(self):
        """Return the last EOD trade data for the security.

        Note
        ----
        This test relies heavily on ``test_get_eod_trade_series`` passing.
        """
        # Update all data instances: Listed & TradeEOD. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        Listed.update_all(
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            _test_isin_list=[self.isin, self.isin1, self.isin2])
        # Test for AAPL Inc.
        listed = Listed.factory(self.session, self.isin)
        # Method to be tested
        last_dict = listed.get_last_eod_trades()
        # Test values
        last_dict_test = listed.get_eod_trade_series().iloc[-1].to_dict()
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
        cls.get_dividends_method = SecuritiesHistory().get_dividends
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
            self.status)
        self.assertIsInstance(listed, ListedEquity)
        self.assertEqual(listed.name, self.name)
        self.assertEqual(listed.issuer, issuer)
        self.assertEqual(listed.isin, self.isin)
        self.assertEqual(listed.exchange.mic, self.mic)
        self.assertEqual(listed.ticker, self.ticker)
        # Check domicile, against ISIN, against issuer arguments.
        self.assertEqual(listed.domicile.code, self.isin[0:2])
        self.assertEqual(listed.domicile.code, self.issuer_domicile_code)
        self.assertEqual(listed.domicile, listed.issuer.domicile)
        # Check status
        self.assertEqual(listed.status, listed.status)

        # Test with industry classification
        listed = ListedEquity(
            self.name,  issuer, self.isin, self.exchange, self.ticker,
            self.status,
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
        self.assertEqual(listed.domicile.code, self.isin[0:2])
        self.assertEqual(listed.domicile.code, self.issuer_domicile_code)
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
        self.assertEqual(listed.domicile.code, self.isin[0:2])
        self.assertEqual(listed.domicile.code, self.issuer_domicile_code)
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
        self.assertEqual(listed.domicile.code, self.isin[0:2])
        self.assertEqual(listed.domicile.code, self.issuer_domicile_code)
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
        self.assertEqual(listed.domicile.code, self.isin[0:2])
        self.assertEqual(listed.domicile.code, self.issuer_domicile_code)
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

    def test_update_all_trade_eod_and_dividends(self):
        """Update all Listed, TradeEOD, Dividend objs from getter methods."""
        # Insert only selected subset of securities meta-data. Update all data
        # instances: ListedEquity, TradeEOD & Dividend. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        ListedEquity.update_all(  # Method to be tested
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            get_dividends_method=self.get_dividends_method,
            _test_isin_list=[self.isin, self.isin1, self.isin2])

        # Retrieve the submitted TradeEOD data from entitybase
        df = pd.DataFrame([self.to_eod_dict(item)
                           for item in self.session.query(TradeEOD).all()])
        # Test
        df['date_stamp'] = pd.to_datetime(df['date_stamp'])
        df.sort_values(['date_stamp', 'ticker'], inplace=True)
        # Test against last date test_values data
        last_date = datetime.datetime.strptime(self.to_date, '%Y-%m-%d')
        df = df[df['date_stamp'] == last_date]
        self.assertFalse(df.empty)
        # Exclude adjusted_close as it changes
        df = df[self.test_columns]  # Column select and rank for testing
        df.reset_index(drop=True, inplace=True)
        pd.testing.assert_frame_equal(self.test_values, df, check_dtype=False)

        df = pd.DataFrame([self.to_dividend_dict(item)
                           for item in self.session.query(Dividend).all()])
        # BUG : Is empty df
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
        # instances: ListedEquity, TradeEOD & Dividend. Force a limited set of 3
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
        # instances: ListedEquity, TradeEOD & Dividend. Force a limited set of 3
        # securities by using the _test_isin_list keyword argument.
        ListedEquity.update_all(  # Method to be tested
            self.session, self.get_meta_method,
            get_eod_method=self.get_eod_method,
            get_dividends_method=self.get_dividends_method,
            _test_isin_list=[self.isin2])
        # Test for AAPL Inc.
        listed = ListedEquity.factory(self.session, self.isin2)
        # Method to be tested
        df = listed.time_series()
        # Check data
        # NOTE: These values may change as EOD historical data gets corrected
        # FIXME: Check total_returns/total_prices are accurate!!!
        self.assertEqual(df[self.to_date], 54.60)
        df = listed.time_series(series='distribution')
        self.assertEqual(df[self.from_date:self.to_date][-1], 0.091925)
        df = listed.time_series(series='volume')
        self.assertEqual(df[self.to_date], 112700)
        df = listed.time_series(return_type='total_price')
        self.assertEqual(df[self.to_date], 81.24353503753133)
        # Check security name
        self.assertEqual(df.name.identity_code, listed.identity_code)


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
        # Each test with a clean entitybase
        self.entitybase = EntityBase(dialect='memory')
        self.session = self.entitybase.session
        # Add all initialization objects to entitybase
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
            self.status)
        ts_item = TimeSeriesBase(self.listed, date_stamp=datetime.date.today())
        self.assertIsInstance(ts_item, TimeSeriesBase)
        self.assertEqual(ts_item._entity_id, self.listed.id)
        self.assertEqual(ts_item.date_stamp, datetime.date.today())

    def test_unique_constraint(self):
        """Fail the UniqueConstraint('_listed_id', 'date_stamp')"""
        from sqlalchemy.exc import IntegrityError
        # Get AAPL Inc instance form committed instances. Don't create this in
        # `setUp` as this risk mix-ups in the child test classes.
        self.listed = Listed(
            self.name,  self.issuer, self.isin, self.exchange, self.ticker,
            self.status)
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
        # Securities entitybase instances list
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
        self.assertEqual(ts_item._entity_id, listed.id)
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
        TradeEOD.from_data_frame(self.session, data_frame=df)
        # Retrieve the submitted date stamped data from entitybase
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
        ts_last_date = TradeEOD.assert_last_dates(self.session)
        self.assertEqual(ts_last_date, df_last_date)

    def test_to_data_frame(self):
        """Convert all instances to a single data table."""
        # This test is stolen from test_financial_data
        # Call API for data
        test_df = self.feed.get_eod(
            self.securities_list, self.from_date, self.to_date)
        # Call the tested method.
        TradeEOD.from_data_frame(self.session, data_frame=test_df)
        # Method to be tested
        df = TradeEOD.to_data_frame(self.session)
        # Test - first aligning rows and columns
        df.sort_values(by=['isin', 'date_stamp'], inplace=True)
        test_df.sort_values(by=['isin', 'date_stamp'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        test_df.reset_index(drop=True, inplace=True)
        test_df = test_df[df.columns]  # Align columns rank
        pd.testing.assert_frame_equal(test_df, df)

    def test_update_all(self):
        """ Update/create all the objects in the entitybase session."""
        # This test is stolen from test_financial_data
        # Call the tested method.
        TradeEOD.update_all(self.session, self.feed.get_eod)
        # Retrieve the submitted date stamped data from entitybase
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
        # Securities entitybase instances list
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
        self.assertEqual(ts_item._entity_id, listed_equity.id)
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
        Dividend.from_data_frame(self.session, data_frame=df)
        # Retrieve the submitted date stamped data from entitybase
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
        ts_last_date = Dividend.assert_last_dates(self.session)
        self.assertEqual(ts_last_date, df_last_date)

    def test_to_data_frame(self):
        """Convert all instances to a single data table."""
        # This test is stolen from test_financial_data
        # Call API for data
        test_df = self.feed.get_dividends(
            self.securities_list, self.from_date, self.to_date)
        # Call the tested method.
        Dividend.from_data_frame(self.session, data_frame=test_df)
        # Method to be tested
        df = Dividend.to_data_frame(self.session)
        # Test
        df.sort_values(by=['isin', 'date_stamp'], inplace=True)
        test_df.sort_values(by=['isin', 'date_stamp'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        test_df.reset_index(drop=True, inplace=True)
        test_df = test_df[df.columns]  # Align columns rank
        pd.testing.assert_frame_equal(test_df, df)

    def test_update_all(self):
        """Get historical dividends for a specified list of securities."""
        # Test stolen from test_financial_data
        # Call the tested method.
        Dividend.update_all(self.session, self.feed.get_dividends)
        # Retrieve the submitted date stamped data from entitybase
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


class TestModel(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = Model
        # Test strings
        cls.currency_code = 'ZZZ'
        cls.currency_name = 'The Currency'
        cls.domicile_code = 'ZZ'
        cls.domicile_name = 'The Place'
        cls.issuer_name = 'The Corporation'
        cls.model_name = 'The Model'
        cls.ticker = 'TICK'
        cls.identity_code = 'TICK'
        cls.name_child1 = 'The Child Entity 1'
        cls.name_child2 = 'The Child Entity 2'
        cls.name_child3 = 'The Child Entity 3'
        cls.name_child4 = 'The Child Entity 4'
        cls.name_child5 = 'The Child Entity 5'
        cls.name_child6 = 'The Child Entity 6'
        cls.weight1 = 0.8
        cls.weight2 = 0.2
        cls.weight3 = 0.6
        cls.weight4 = 0.4
        cls.weight5 = 0.6
        cls.weight6 = 0.4
        cls.parameters = {
            'parameter1': '100.0',
            'parameter2': '200.0'
        }
        cls.test_str = {
            'parameter1': 'Model parameter "parameter1" = "100.0"',
            'parameter2': 'Model parameter "parameter2" = "200.0"'
        }
        cls.test_str = \
            'Model The Model (TICK) issued by The Corporation in The Place.'

    def setUp(self):
        """Set up test case fixtures."""
        self.session = TestSession().session

    def test___init__(self):
        """Instance initialization."""
        currency = Currency(self.currency_code, self.currency_name)
        domicile = Domicile(self.domicile_code, self.domicile_name,
                            currency)
        issuer = Issuer(self.issuer_name, domicile)
        # Create model parameters.
        parameters = list()
        for name, value in self.parameters.items():
            parameters.append(ModelParameter(name, value))
        # Create child entities.
        child1 = Entity(self.name_child1, domicile)
        child2 = Entity(self.name_child2, domicile)
        # Create parent entity.
        model = Model(self.model_name, issuer, self.ticker,
                      parameters=parameters)
        # Create EntityWeight instances.
        entity_weight1 = EntityWeight(model, child1, self.weight1)
        entity_weight2 = EntityWeight(model, child2, self.weight2)
        model.children_weights = [entity_weight1, entity_weight2]
        self.assertIsInstance(model, Model)
        self.assertIsInstance(entity_weight1, EntityWeight)
        self.assertIsInstance(entity_weight2, EntityWeight)
        self.session.add(model)
        self.session.commit()

    def test___str__(self):
        """Strings"""
        currency = Currency(self.currency_code, self.currency_name)
        domicile = Domicile(self.domicile_code, self.domicile_name,
                            currency)
        issuer = Issuer(self.issuer_name, domicile)
        # Create model parameters.
        parameters = list()
        for name, value in self.parameters.items():
            parameters.append(ModelParameter(name, value))
        # Create child entities.
        child1 = Entity(self.name_child1, domicile)
        child2 = Entity(self.name_child2, domicile)
        # Create parent entity.
        model = Model(self.model_name, issuer, self.ticker,
                      parameters=parameters)
        # Create EntityWeight instances.
        entity_weight1 = EntityWeight(model, child1, self.weight1)
        entity_weight2 = EntityWeight(model, child2, self.weight2)
        model.children_weights = [entity_weight1, entity_weight2]
        self.assertIsInstance(model, Model)
        self.assertIsInstance(entity_weight1, EntityWeight)
        self.assertIsInstance(entity_weight2, EntityWeight)
        self.session.add(model)
        self.session.commit()
        # Strings.
        self.assertEqual(
            model.children_weights[0].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.8000)->(The Child Entity 1 is an entity in The Place.)')
        self.assertEqual(
            model.children_weights[1].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.2000)->(The Child Entity 2 is an entity in The Place.)')

    def test_parameters(self):
        """Test parameters."""
        currency = Currency(self.currency_code, self.currency_name)
        domicile = Domicile(self.domicile_code, self.domicile_name,
                            currency)
        issuer = Issuer(self.issuer_name, domicile)
        # Create model parameters.
        parameters = list()
        for name, value in self.parameters.items():
            parameters.append(ModelParameter(name, value))
        # Create child entities.
        child1 = Entity(self.name_child1, domicile)
        child2 = Entity(self.name_child2, domicile)
        # Create parent entity.
        model = Model(self.model_name, issuer, self.ticker,
                      parameters=parameters)
        # Create EntityWeight instances.
        entity_weight1 = EntityWeight(model, child1, self.weight1)
        entity_weight2 = EntityWeight(model, child2, self.weight2)
        model.children_weights = [entity_weight1, entity_weight2]
        self.assertIsInstance(model, Model)
        self.assertIsInstance(entity_weight1, EntityWeight)
        self.assertIsInstance(entity_weight2, EntityWeight)
        self.session.add(model)
        self.session.commit()
        # Check parameters.
        parameters = model.get_parameter_dict()
        for name, value in self.parameters.items():
            self.assertEqual(parameters[name], self.parameters[name])

    def test_factory(self):
        """Factory adds children weights."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 2)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights, parameters=self.parameters)
        #  Check issuer exists.
        self.assertEqual(len(self.session.query(Issuer).all()), 1)
        #  Retrieve and reconcile.
        obj = Model.factory(self.session, self.ticker, self.issuer_name,
                            self.domicile_code, create=False)
        self.assertEqual(obj.__str__(), self.test_str)
        # Test model weights.
        weights = obj.get_weights()
        weight_dict = dict()
        for entity, value in weights:
            weight_dict[entity.id] = value
        for id, value in children_weights.items():
            self.assertEqual(weight_dict[id], value)
        # Test model parameters.
        parameters = obj.get_parameter_dict()
        for name, value in self.parameters.items():
            self.assertEqual(parameters[name], value)

    def test_factory_update(self):
        """Factory adds children weights then updates them."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        en3 = Entity.factory(self.session, self.name_child3, self.domicile_code)
        en4 = Entity.factory(self.session, self.name_child4, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 4)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights)
        #  Check issuer exists.
        self.assertEqual(len(self.session.query(Issuer).all()), 1)
        #  Retrieve and reconcile.
        obj = Model.factory(self.session, self.ticker, self.issuer_name,
                            self.domicile_code)
        # Strings.
        self.assertEqual(
            obj.children_weights[0].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.8000)->(The Child Entity 1 is an entity in The Place.)')
        self.assertEqual(
            obj.children_weights[1].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.2000)->(The Child Entity 2 is an entity in The Place.)')
        # Changed children weight dictionary.
        children_weights = {
            en3.id: self.weight3,
            en4.id: self.weight4,
        }
        #  Retrieve and update.
        obj = Model.factory(self.session, self.ticker, self.issuer_name,
                            self.domicile_code, self.model_name,
                            weights=children_weights, update=True)
        self.session.commit()
        # Strings.
        self.assertEqual(
            obj.children_weights[0].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.6000)->(The Child Entity 3 is an entity in The Place.)')
        self.assertEqual(
            obj.children_weights[1].__str__(),
            '(Model The Model (TICK) issued by The Corporation in The Place.)-\
                holds(0.4000)->(The Child Entity 4 is an entity in The Place.)')

    def test_factory_fail_reconcile0(self):
        """The children dictionary parameter size doesn't reconcile."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        en3 = Entity.factory(self.session, self.name_child3, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 3)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights)
        #  Retrieve and reconcile.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
            en3.id: self.weight3,
        }
        with self.assertRaises(ReconcileError) as fail:
            # Retrieve.
            Model.factory(self.session, self.ticker, self.issuer_name,
                          self.domicile_code, self.model_name,
                          weights=children_weights)
        self.assertEqual(fail.exception.message,
                         'The children:number does not reconcile.')

    def test_factory_fail_reconcile1(self):
        """The children dictionary parameter key doesn't reconcile."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        en3 = Entity.factory(self.session, self.name_child3, self.domicile_code)
        en4 = Entity.factory(self.session, self.name_child4, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 4)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights)
        #  Retrieve and reconcile.
        children_weights = {
            en3.id: self.weight3,
            en4.id: self.weight4,
        }
        with self.assertRaises(ReconcileError) as fail:
            # Retrieve.
            Model.factory(
                self.session, self.ticker, self.issuer_name,
                self.domicile_code, self.model_name,
                weights=children_weights)
        self.assertEqual(fail.exception.message,
                         'The children:key does not reconcile.')

    def test_factory_fail_reconcile2(self):
        """The children dictionary parameter weight doesn't reconcile."""
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add children.
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code)
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 4)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights)
        #  Retrieve and reconcile.
        children_weights = {
            en1.id: self.weight3,
            en2.id: self.weight4,
        }
        with self.assertRaises(ReconcileError) as fail:
            # Retrieve.
            Model.factory(
                self.session, self.ticker, self.issuer_name,
                self.domicile_code, self.model_name,
                weights=children_weights)
        self.assertEqual(fail.exception.message,
                         'The weight does not reconcile.')

    def test_weights(self):
        """Compute weights of all entities in a holding. Use factory method.

        Create holding tree:

                      Parent vertice
                    /   \    Weighted edges.
          Child    1     2   Internal vertices.
                  / \   / \  Weighted edges.
          Child  3  4  5  6  Leaf vertices
        """
        # Pre-add currency.
        Currency.factory(self.session, self.currency_code, self.currency_name)
        # Pre-add domicile with currency already added.
        Domicile.factory(self.session, self.domicile_code, self.domicile_name,
                         self.currency_code)
        # Pre-add leaf children.
        en3 = Entity.factory(self.session, self.name_child3, self.domicile_code)
        en4 = Entity.factory(self.session, self.name_child4, self.domicile_code)
        en5 = Entity.factory(self.session, self.name_child5, self.domicile_code)
        en6 = Entity.factory(self.session, self.name_child6, self.domicile_code)
        self.assertEqual(len(self.session.query(Entity).all()), 4)
        # Children weight dictionary.
        children_weights1 = {
            en3.id: self.weight3,
            en4.id: self.weight4,
        }
        en1 = Entity.factory(self.session, self.name_child1, self.domicile_code,
                             children=children_weights1)
        # Child 2.
        children_weights2 = {
            en5.id: self.weight5,
            en6.id: self.weight6,
        }
        en2 = Entity.factory(self.session, self.name_child2, self.domicile_code,
                             children=children_weights2)
        # Children weight dictionary.
        children_weights = {
            en1.id: self.weight1,
            en2.id: self.weight2,
        }
        # Add model.
        Model.factory(self.session, self.ticker, self.issuer_name,
                      self.domicile_code, self.model_name,
                      weights=children_weights)
        # Test
        answer = {
            'The Model': 1.00,
            'The Child Entity 1': 0.80,
            'The Child Entity 2': 0.20,
            'The Child Entity 3': 0.48,
            'The Child Entity 4': 0.32,
            'The Child Entity 5': 0.12,
            'The Child Entity 6': 0.08,
        }
        #  Get parent.
        obj = Model.factory(self.session, self.ticker, self.issuer_name,
                            self.domicile_code)
        #  All.
        result = obj.get_weights(which='all')
        for (entity, weight) in result:
            name = entity.name
            self.assertAlmostEqual(answer[name], weight, 15)
        # Leaves.
        result = obj.get_weights(which='leaves')
        for (entity, weight) in result:
            name = entity.name
            self.assertAlmostEqual(answer[name], weight, 15)


class TestModelParameter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = ModelParameter
        # Parameters
        cls.parameters = {
            'parameter1': 100.0,
            'parameter2': 200.0
        }
        cls.test_str = {
            'parameter1': 'Model parameter "parameter1" = "100.0"',
            'parameter2': 'Model parameter "parameter2" = "200.0"'
        }

    def setUp(self):
        """Set up test case fixtures."""
        self.session = TestSession().session

    def test___init__(self):
        """Instance initialization."""
        for name, value in self.parameters.items():
            parameter = ModelParameter(name, value)
            self.assertIsInstance(parameter, ModelParameter)

    def test___str__(self):
        """Strings"""
        for name, value in self.parameters.items():
            parameter = ModelParameter(name, value)
            self.assertEqual(parameter.__str__(), self.test_str[name])


class TestEntityBaseSetUp(unittest.TestCase):
    """Set up and tear down the entitybase manager.

    This test is complex and different enough that it warrants it's own test.
    """

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        # Specify which class is being tested. Apply when tests are meant to be
        # inherited.
        cls.Cls = EntityBase

        # Make a memory based entitybase session with test data.
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
        self.entitybase = EntityBase(dialect='memory', testing=True)
        self.session = self.entitybase.session

        # For a fresh test delete any previously dumped data, but keep the dump
        # folder
        self.entitybase.delete_dumps(delete_folder=False)

        # Set-up the database with some securities
        self.entitybase.set_up(
            _test_isin_list=[self.isin, self.isin1, self.isin2],
            )

    def tearDown(self):
        """Tear down test case fixtures."""
        # Tear down entitybase and delete the dump folder and its contents.
        self.entitybase.tear_down(delete_dump_data=True)

    def test___init__(self):
        """Instance initialization."""
        self.assertIsInstance(self.entitybase, EntityBase)

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
        self.entitybase.tear_down()

        # Re-set-up the database again with some securities. This should reuse
        # the dump data and have a much younger from_date in the feed API
        # fetches
        self.entitybase.set_up(
            _test_isin_list=[self.isin, self.isin1, self.isin2]
            )

        # The session will have been closed by the `set_up` method this
        # preventing any further ORM based lazy object attribute loading.
        # Therefore we need to call for new object form the new session created
        # in `set_up`.
        self.session = self.entitybase.session  # Get the fresh session

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
    """The entitybase manager."""

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

        # Make a memory based entitybase session with test data.
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
        cls.entitybase = EntityBase(dialect='memory', testing=True)
        cls.session = cls.entitybase.session
        cls.entitybase.set_up(_test_isin_list=cls.isin_list)

        # Get financial asset entities and cash currency
        cls.security_list = cls.session.query(Listed).filter(Listed.isin.in_(cls.isin_list)).all()
        cls.cash_list = cls.session.query(Cash).filter(Cash.ticker.in_(cash_ticker_list)).all()
        cls.asset_list = cls.security_list + cls.cash_list

    @classmethod
    def tearDownClass(cls):
        """ Tear down class test fixtures. """
        # Delete test dump folder and its contents.
        # Tear down entitybase and delete the dump folder and its contents.
        cls.entitybase.tear_down(delete_dump_data=True)

    def setUp(self):
        """Set up test case fixtures."""
        pass

    def tearDown(self):
        """Tear down test case fixtures."""
        pass

    def test___init__(self):
        """Instance initialization."""
        self.assertIsInstance(self.entitybase, EntityBase)

    def test_time_series(self):
        """Cash and non-cash securities."""
        # Retrieve time series
        data = self.entitybase.time_series(self.asset_list)
        data_tickers = entitybase.replace_time_series_labels(data, 'ticker')
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
        data_id_code = entitybase.replace_time_series_labels(
            data, 'identity_code')
        columns_data = [s.identity_code for s in self.asset_list]
        columns_id_code = [s for s in data_id_code.columns]
        # FIXME: Test has weird currency ticker `PW.USD`. WTF!!
        # ['ZAE000027108.STX40', 'US0378331005.AAPL', 'US5801351017.MCD', 'PW.USD', 'ZA.ZAR']
        import ipdb; ipdb.set_trace()


class Suite(object):
    """For running the complete test suite.

    Run this with the command:

        ``python -m fundmanage.tests.test_entitybase``

    """

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # Classes that are passing. Add the others later when they too work.
        test_classes = [
            TestCurrency,
            TestDomicile,
            TestEntity,
            TestTimeSeriesBase,
            TestInstitution,
            TestIssuer,
            TestExchange,
            TestAsset,
            TestCash,
            TestShare,
            TestListed,
            TestListedEquity,
            TestTradeEOD,
            TestDividend,
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
