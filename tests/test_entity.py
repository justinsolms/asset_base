import unittest
import pandas as pd

from ..financial_data import Static
from ..common import TestSession
from ..exceptions import FactoryError, ReconcileError
from ..entity import Currency, Domicile
from ..entity import Entity, Exchange


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
        # A single currency  - a list of countries use USD
        cls.currency_item = cls.currency_dataframe[
            cls.currency_dataframe.ticker == 'USD']
        cls.ticker = cls.currency_item.ticker.to_list()[0]
        cls.name = cls.currency_item.name.to_list()[0]
        cls.country_code_list = cls.currency_item.country_code_list.to_list()[0]
        # A second single currency - a list of countries use GBP
        cls.currency_item1 = cls.currency_dataframe[
            cls.currency_dataframe.ticker == 'GBP']
        cls.ticker1 = cls.currency_item1.ticker.to_list()[0]
        cls.name1 = cls.currency_item1.name.to_list()[0]
        cls.country_code_list1 = cls.currency_item1.country_code_list.to_list()[0]

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean sqlite in-memory database
        self.session = TestSession().session

    def test___init__(self):
        """Initialization."""
        # Use GBP - a list of countries use GBP
        obj = Currency(
            ticker=self.ticker, name=self.name,
            country_code_list=self.country_code_list)
        self.assertIsInstance(obj, Currency)
        self.assertEqual(obj.ticker, self.ticker)
        self.assertEqual(obj.name, self.name)
        self.assertEqual(obj.country_code_list, self.country_code_list)

    def test___str__(self):
        """String output."""
        obj = Currency(
            ticker=self.ticker, name=self.name,
            country_code_list=self.country_code_list)
        self.assertEqual(obj.__str__(), 'Currency is U.S. Dollar (USD)')

    def test_key_code(self):
        obj = Currency(
            ticker=self.ticker, name=self.name,
            country_code_list=self.country_code_list)
        self.assertEqual(obj.key_code, 'USD')

    def test_identity_code(self):
        obj = Currency(
            ticker=self.ticker, name=self.name,
            country_code_list=self.country_code_list)
        self.assertEqual(obj.identity_code, 'USD')

    # Write a test for Currency.in_domicile
    def test_in_domicile(self):
        """Check if the currency is domiciled in the specified country."""
        obj = Currency(
            ticker=self.ticker, name=self.name,
            country_code_list=self.country_code_list)
        self.assertTrue(obj.in_domicile('US'))
        self.assertFalse(obj.in_domicile('GB'))

    def test_factory(self):
        """Factory create."""
        # Despite using factory twice there should be only one instance
        obj = Currency.factory(
            self.session, self.ticker, self.name,
            country_code_list=self.country_code_list)
        obj = Currency.factory(
            self.session, self.ticker, self.name,
            country_code_list=self.country_code_list)
        self.assertEqual(len(self.session.query(Currency).all()), 1)
        self.assertEqual(obj.ticker, self.ticker)
        self.assertEqual(obj.name, self.name)

    def test_factory_fail_create(self):
        """Fail create new with no currency name provided."""
        with self.assertRaises(FactoryError):
            Currency.factory(self.session, self.ticker)

    def test_factory_change(self):
        """Currency name changed."""
        obj = Currency.factory(self.session, self.ticker, self.name, country_code_list=self.country_code_list)
        new_name = 'A Changed Currency Name for Testing'
        with self.assertRaises(FactoryError):
            obj = Currency.factory(
                self.session, self.ticker, new_name,
                country_code_list=self.country_code_list)

    def test_factory_fail(self):
        """Instance Factory Fails."""
        with self.assertRaises(FactoryError):
            # Non-existent instance need currency_name argument to create
            Currency.factory(self.session, self.ticker)

    def test_from_data_frame(self):
        """Get data from a pandas.DataFrame."""
        Currency.from_data_frame(self.session, self.currency_dataframe)
        # Test a currency
        obj = Currency.factory(self.session, self.ticker, country_code_list=self.country_code_list)
        self.assertEqual(obj.ticker, self.ticker)
        self.assertEqual(obj.name, self.name)
        # Test a a second currency
        obj = Currency.factory(self.session, self.ticker1, country_code_list=self.country_code_list1)
        self.assertEqual(obj.ticker, self.ticker1)
        self.assertEqual(obj.name, self.name1)

    def test_update_all(self):
        """Create/update all Currency objects from the financial_data module"""
        Currency.update_all(self.session, self.get_method)
        # Test a currency
        obj = Currency.factory(self.session, self.ticker, country_code_list=self.country_code_list)
        self.assertEqual(obj.ticker, self.ticker)
        self.assertEqual(obj.name, self.name)
        # Test a a second currency
        obj = Currency.factory(self.session, self.ticker1, country_code_list=self.country_code_list1)
        self.assertEqual(obj.ticker, self.ticker1)
        self.assertEqual(obj.name, self.name1)


class TestDomicile(unittest.TestCase):
    """Domicile and related Currency

    Note
    ----
    All Currency object instances shall be in asset_base before any dependent
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
            cls.domicile_dataframe.country_code == 'US']
        cls.country_code = cls.domicile_item.country_code.to_list()[0]
        cls.country_name = cls.domicile_item.country_name.to_list()[0]
        cls.currency_ticker = cls.domicile_item.currency_ticker.to_list()[0]
        # A second single domicile with currency
        cls.domicile_item1 = cls.domicile_dataframe[
            cls.domicile_dataframe.country_code == 'GB']
        cls.country_code1 = cls.domicile_item1.country_code.to_list()[0]
        cls.country_name1 = cls.domicile_item1.country_name.to_list()[0]
        cls.currency_ticker1 = cls.domicile_item1.currency_ticker.to_list()[0]

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean sqlite in-memory session
        self.session = TestSession().session
        # Add all currency objects to asset_base
        Currency.update_all(self.session, get_method=Static().get_currency)
        self.currency = Currency.factory(self.session, self.currency_ticker)
        self.currency1 = Currency.factory(self.session, self.currency_ticker1)

    def test___init__(self):
        domicile = Domicile(self.country_code,
                            self.country_name, self.currency)
        self.assertIsInstance(domicile, Domicile)
        self.assertEqual(domicile.country_code, self.country_code)
        self.assertEqual(domicile.country_name, self.country_name)
        self.assertEqual(domicile.currency, self.currency)

    def test_key_code(self):
        domicile = Domicile(self.country_code,
                            self.country_name, self.currency)
        self.assertEqual(domicile.key_code, 'US')

    def test_identity_code(self):
        domicile = Domicile(self.country_code,
                            self.country_name, self.currency)
        self.assertEqual(domicile.identity_code, 'US')

    def test___str__(self):
        domicile = Domicile(self.country_code,
                            self.country_name, self.currency)
        self.assertEqual(domicile.__str__(), 'Domicile is United States (US)')

    def test_factory(self):
        """Instance Factory."""
        # Add twice, should retrieve one.
        domicile = Domicile.factory(self.session, self.country_code,
                                    self.country_name, self.currency.ticker)
        domicile = Domicile.factory(self.session, self.country_code,
                                    self.country_name, self.currency.ticker)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Domicile).all()), 1)
        self.assertEqual(domicile.country_code, self.country_code)
        self.assertEqual(domicile.country_name, self.country_name)
        self.assertEqual(domicile.currency, self.currency)

    def test_factory_change(self):
        """Instance Factory handles changes."""
        Domicile.factory(self.session, self.country_code, self.country_name,
                         self.currency.ticker)
        # Change domicile name. Change currency
        new_country_name = 'A New Domicile Name for Testing'
        new_currency_code = 'YYY'
        with self.assertRaises(FactoryError):
            Domicile.factory(
                self.session, self.country_code,
                new_country_name, new_currency_code)
        # Bad currency code
        with self.assertRaises(FactoryError):
            # Point to non-existent currency
            Domicile.factory(
                self.session, self.country_code,
                self.country_name, new_currency_code)

    def test_factory_fail(self):
        """Instance Factory Fails."""
        with self.assertRaises(FactoryError):
            # Non-existent instance need full arguments argument to create
            Domicile.factory(self.session, self.country_code)

    def test_from_data_frame(self):
        """Get data from a pandas.DataFrame."""
        Domicile.from_data_frame(
            self.session, data_frame=self.domicile_dataframe)
        # Test a domicile
        obj = Domicile.factory(self.session, self.country_code)
        self.assertEqual(obj.country_code, self.country_code)
        self.assertEqual(obj.country_name, self.country_name)
        # Test a a second domicile
        obj = Domicile.factory(self.session, self.country_code1)
        self.assertEqual(obj.country_code, self.country_code1)
        self.assertEqual(obj.country_name, self.country_name1)

    def test_update_all(self):
        """Create/update all Domicile objects from the financial_data module"""
        Domicile.update_all(self.session, self.get_method)
        # Test a currency
        obj = Domicile.factory(self.session, self.country_code)
        self.assertEqual(obj.country_code, self.country_code)
        self.assertEqual(obj.country_name, self.country_name)
        # Test a a second currency
        obj = Domicile.factory(self.session, self.country_code1)
        self.assertEqual(obj.country_code, self.country_code1)
        self.assertEqual(obj.country_name, self.country_name1)


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
            cls.domicile_dataframe.country_code == 'US']
        cls.country_code = cls.domicile_item.country_code.to_list()[0]
        cls.country_name = cls.domicile_item.country_name.to_list()[0]
        cls.currency_ticker = cls.domicile_item.currency_ticker.to_list()[0]
        cls.name = 'Test Entity'
        cls.test_str = 'Test Entity is an Entity in United States'
        cls.key_code = 'US.Test Entity'
        cls.identity_code = 'US.Test Entity'

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean sqlite in-memory session
        self.session = TestSession().session
        # Add all Currency objects to asset_base
        Currency.update_all(self.session, get_method=Static().get_currency)
        # Add all Domicile objects to the asset_base
        Domicile.update_all(self.session, get_method=Static().get_domicile)
        self.domicile = Domicile.factory(self.session, self.country_code)

    def test___init__(self):
        entity = Entity(self.name, self.domicile)
        self.assertIsInstance(entity, Entity)
        # Attributes
        self.assertEqual(entity.name, self.name)
        self.assertEqual(entity.domicile.country_code, self.domicile.country_code)

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
        entity = Entity.factory(self.session, self.name, self.country_code)
        entity = Entity.factory(self.session, self.name, self.country_code)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Entity).all()), 1)
        # Attributes
        self.assertEqual(entity.name, self.name)
        self.assertEqual(entity.domicile.country_code, self.domicile.country_code)
        # Get
        entity1 = Entity.factory(self.session, self.name, self.country_code)
        self.assertEqual(entity, entity1)

    def test_factory_fail(self):
        """Test session add fail if second add has wrong country_name."""
        with self.assertRaises(FactoryError):
            wrong_country_code = '##'
            Entity.factory(self.session, self.name, wrong_country_code)

    def test_factory_no_create(self):
        """Test create parameter."""
        # Add.
        with self.assertRaises(FactoryError):
            Entity.factory(self.session, self.name, self.country_code,
                           create=False)

    def test_update_all(self):
        """Update all data form a getter method."""
        assert True  # FIXME: We don't test this yet.

    def test_key_code_id_table(self):
        """A table of all instance's ``Entity.id`` against ``key_code``."""
        Entity.factory(self.session, self.name, self.country_code)
        instances_list = self.session.query(Entity).all()
        test_df = pd.DataFrame(
            [(item.id, item.key_code) for item in instances_list],
            columns=['id', 'key_code'])
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
        cls.country_code = cls.exchange_item.country_code.to_list()[0]
        cls.eod_code = cls.exchange_item.eod_code.to_list()[0]
        cls.test_str = 'USA Stocks (XNYS) is an Exchange in United States'

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()

    def test___init__(self):
        exchange = Exchange(
            self.exchange_name, self.domicile, self.mic, eod_code=self.eod_code)
        self.assertIsInstance(exchange, Exchange)
        # Attributes
        self.assertEqual(exchange.name, self.exchange_name)
        self.assertEqual(exchange.domicile.country_code, self.domicile.country_code)
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
            self.session, self.mic, self.exchange_name, self.country_code,
            eod_code=self.eod_code)
        exchange = Exchange.factory(
            self.session, self.mic, self.exchange_name, self.country_code,
            eod_code=self.eod_code)
        # Despite using factory twice there should be only one instance
        self.assertEqual(len(self.session.query(Exchange).all()), 1)
        # Attributes
        self.assertEqual(exchange.name, self.exchange_name)
        self.assertEqual(exchange.domicile.country_code, self.domicile.country_code)
        self.assertEqual(exchange.mic, self.mic)
        self.assertEqual(exchange.eod_code, self.eod_code)
        # Get is same
        exchange1 = Exchange.factory(self.session, mic=self.mic)
        self.assertEqual(exchange, exchange1)

    def test_factory_change(self):
        """Instance Factory handles changes."""
        # Add.
        Exchange.factory(
            self.session, self.mic, self.exchange_name, self.country_code,
            eod_code=self.eod_code)
        # Changes
        with self.assertRaises(ReconcileError):
            Exchange.factory(
                self.session, self.mic, exchange_name='newname')
        with self.assertRaises(ReconcileError):
            Exchange.factory(
                self.session, self.mic, country_code='newcode')

    def test_factory_fail(self):
        """Test session add fail if second add has wrong domicile_name."""
        with self.assertRaises(FactoryError):
            wrong_country_code = '##'
            Exchange.factory(
                self.session, self.mic, self.exchange_name, wrong_country_code,
                eod_code=self.eod_code)

    def test_from_data_frame(self):
        """Get data from a pandas.DataFrame."""
        Exchange.from_data_frame(
            self.session, data_frame=self.exchange_dataframe)
        exchange = Exchange.factory(self.session, self.mic)
        # Attributes
        self.assertEqual(exchange.name, self.exchange_name)
        self.assertEqual(exchange.domicile.country_code, self.domicile.country_code)
        self.assertEqual(exchange.mic, self.mic)
        self.assertEqual(exchange.eod_code, self.eod_code)

    def test_update_all(self):
        """Create/update all Domicile objects from the financial_data module"""
        Exchange.update_all(self.session, self.get_method)
        exchange = Exchange.factory(self.session, self.mic)
        # Attributes
        self.assertEqual(exchange.name, self.exchange_name)
        self.assertEqual(exchange.domicile.country_code, self.domicile.country_code)
        self.assertEqual(exchange.mic, self.mic)
        self.assertEqual(exchange.eod_code, self.eod_code)

    def test_factory_no_create(self):
        """Test create parameter."""
        # Add.
        with self.assertRaises(FactoryError):
            Exchange.factory(
                self.session, self.mic, self.exchange_name, self.country_code,
                eod_code=self.eod_code, create=False)


class Suite(object):
    """Test suite"""

    def __init__(self):
        """Initialization."""
        suite = unittest.TestSuite()

        # Classes that are passing. Add the others later when they too work.
        test_classes = [
            TestCurrency,
            TestDomicile,
            TestEntity,
            TestInstitution,
            TestIssuer,
            TestExchange,
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
