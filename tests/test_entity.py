import unittest
import pandas as pd

from src.asset_base.financial_data import Static
from src.asset_base.common import TestSession
from src.asset_base.exceptions import FactoryError, ReconcileError
from src.asset_base.entity import Currency, Domicile, Issuer
from src.asset_base.entity import Entity, Exchange


class TestBase(unittest.TestCase):

    """A test base with common test fixtures."""

    @classmethod
    def setUpClass(cls):
        """Set up class-wide test fixtures."""
        cls.name = "Test Entity"
        cls.currency_ticker = "USD"
        cls.domicile_ticker = "US"
        cls.issuer_name = "Test Issuer"
        cls.issuer_domicile_code = "US"
        cls.exchange_ticker = "XNYS"

    def setUp(self):
        """Set up test fixtures."""
        # Each test with a clean sqlite in-memory database
        self.test_session = TestSession()
        self.session = self.test_session.session
        # Add all Currency objects to asset_base
        Currency.update_all(self.session, get_method=Static().get_currency)
        self.currency = Currency.factory(self.session, self.currency_ticker)
        # Add all Domicile objects to asset_base
        Domicile.update_all(self.session, get_method=Static().get_domicile)
        self.domicile = Domicile.factory(self.session, self.domicile_ticker)
        # Add an Issuer object to asset_base
        self.issuer = Issuer.factory(self.session, self.issuer_name, self.issuer_domicile_code)
        # Add an Exchange object to asset_base
        Exchange.update_all(self.session, get_method=Static().get_exchange)
        self.exchange = Exchange.factory(self.session, self.exchange_ticker)

    def tearDown(self) -> None:
        """Tear down test case fixtures."""
        self.test_session.close()


class TestCurrency(TestBase):
    """Test the Currency class."""

    def setUp(self):
        """Set up test case fixtures."""
        # Each test with a clean sqlite in-memory database
        super().setUp()
        # Use USD currency from TestBase fixtures

    def test_class_initialization(self):
        """Test class initialization."""
        currency = Currency(
            ticker="EUR",
            name="Euro",
            country_code_list="AT,BE,CY,EE,FI,FR,DE,GR,IE,IT,LV,LT,LU,MT,NL,PT,SK,SI,ES"
        )
        self.assertIsInstance(currency, Currency)
        self.assertEqual(currency.ticker, "EUR")
        self.assertEqual(currency.name, "Euro")

    def test_str_method(self):
        """Test __str__ method returns correct format."""
        result = str(self.currency)
        self.assertIn("Currency", result)
        self.assertIn(self.currency.name, result)
        self.assertIn(self.currency.ticker, result)

    def test_repr_method(self):
        """Test __repr__ method returns correct format."""
        result = repr(self.currency)
        self.assertIn("Currency", result)
        self.assertIn(f'ticker="{self.currency.ticker}"', result)
        self.assertIn(f'name="{self.currency.name}"', result)
        self.assertIn("country_code_list=", result)

    def test_key_code_property(self):
        """Test key_code property returns ticker."""
        self.assertEqual(self.currency.key_code, self.currency_ticker)

    def test_identity_code_property(self):
        """Test identity_code property returns ticker."""
        self.assertEqual(self.currency.identity_code, self.currency_ticker)

    def test_class_name_method(self):
        """Test _class_name method returns class name."""
        self.assertEqual(Currency._class_name(), "Currency")

    def test_database_persistence(self):
        """Test that Currency instance can be persisted to database."""
        # Query back from database using pre-loaded currency
        retrieved = self.session.query(Currency).filter(Currency.ticker == self.currency_ticker).first()
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.ticker, self.currency_ticker)
        self.assertIsInstance(retrieved.name, str)
        self.assertIsInstance(retrieved.country_code_list, str)

    def test_unique_ticker_constraint(self):
        """Test that ticker uniqueness is enforced."""
        # Try to add another currency with existing ticker
        currency_duplicate = Currency(
            ticker=self.currency_ticker,  # USD already exists
            name="Different Name",
            country_code_list="XX"
        )
        self.session.add(currency_duplicate)

        from sqlalchemy.exc import IntegrityError
        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_in_domicile_returns_true_for_valid_country(self):
        """Test in_domicile returns True for country in list."""
        # USD should be in US
        self.assertTrue(self.currency.in_domicile(self.domicile_ticker))

    def test_in_domicile_returns_false_for_invalid_country(self):
        """Test in_domicile returns False for country not in list."""
        # USD should not be in Germany
        self.assertFalse(self.currency.in_domicile("DE"))

    def test_in_domicile_checks_multiple_countries(self):
        """Test in_domicile works with multiple countries in list."""
        # Create EUR which has multiple countries
        eur = Currency.factory(self.session, "EUR")
        # EUR should be in France
        self.assertTrue(eur.in_domicile("FR"))
        # EUR should be in Italy
        self.assertTrue(eur.in_domicile("IT"))
        # EUR should be in Spain
        self.assertTrue(eur.in_domicile("ES"))

    def test_factory_creates_instance(self):
        """Test factory method retrieves existing Currency instance."""
        # Factory retrieves pre-loaded currency
        currency_instance = Currency.factory(
            self.session,
            ticker="JPY"  # Already loaded from static files
        )
        self.assertIsInstance(currency_instance, Currency)
        self.assertEqual(currency_instance.ticker, "JPY")

    def test_factory_retrieves_existing_instance(self):
        """Test factory method with existing instance in database."""
        # Get pre-loaded currency
        currency1 = Currency.factory(self.session, ticker=self.currency_ticker)

        # Verify it exists
        count = self.session.query(Currency).filter(Currency.ticker == self.currency_ticker).count()
        self.assertEqual(count, 1)

        # Factory call again should find existing
        currency2 = Currency.factory(self.session, ticker=self.currency_ticker)
        self.assertIsInstance(currency2, Currency)
        self.assertEqual(currency2.ticker, currency1.ticker)
        self.assertEqual(currency2.name, currency1.name)

    def test_factory_requires_3_letter_ticker(self):
        """Test factory asserts 3-letter ticker."""
        with self.assertRaises(AssertionError):
            Currency.factory(
                self.session,
                ticker="TOOLONG",
                name="Invalid Currency",
                country_code_list="XX"
            )

    def test_factory_requires_2_letter_country_codes(self):
        """Test factory asserts 2-letter country codes."""
        with self.assertRaises(AssertionError):
            Currency.factory(
                self.session,
                ticker="XXX",
                name="Invalid Currency",
                country_code_list="USA"  # Should be 2 letters
            )

    def test_factory_raises_error_without_required_params(self):
        """Test factory raises error when creating without required params."""
        with self.assertRaises(FactoryError):
            Currency.factory(
                self.session,
                ticker="XXX"  # Non-existent ticker, missing name and country_code_list
            )

    def test_factory_raises_error_on_name_mismatch(self):
        """Test factory raises error when name doesn't match existing."""
        # Get pre-loaded currency to find actual name
        existing = Currency.factory(self.session, ticker=self.currency_ticker)

        # Try to retrieve with different name
        with self.assertRaises(FactoryError):
            Currency.factory(
                self.session,
                ticker=self.currency_ticker,
                name="Wrong Name"  # Different from stored name
            )

    def test_from_data_frame(self):
        """Test from_data_frame retrieves currencies from dataframe."""
        # Create test dataframe with pre-loaded currencies
        data = pd.DataFrame({
            'ticker': ['NZD', 'SGD', 'HKD'],
        })

        # Process currencies from dataframe (will retrieve existing)
        Currency.from_data_frame(self.session, data)
        self.session.commit()

        # Verify all exist
        nzd = self.session.query(Currency).filter(Currency.ticker == 'NZD').first()
        sgd = self.session.query(Currency).filter(Currency.ticker == 'SGD').first()
        hkd = self.session.query(Currency).filter(Currency.ticker == 'HKD').first()

        self.assertIsNotNone(nzd)
        self.assertIsNotNone(sgd)
        self.assertIsNotNone(hkd)

    def test_update_all_creates_currencies(self):
        """Test update_all creates Currency instances."""
        # Count currencies before
        count_before = self.session.query(Currency).count()

        # Run update_all (already called in setUp, but test it explicitly)
        Currency.update_all(self.session, get_method=Static().get_currency)
        self.session.commit()

        # Count currencies after
        count_after = self.session.query(Currency).count()

        # Should have currencies loaded
        self.assertGreater(count_after, 0)
        # Should not have duplicates (update_all uses factory which checks for existing)
        self.assertEqual(count_before, count_after)

    def test_ticker_property(self):
        """Test ticker property is accessible."""
        self.assertEqual(self.currency.ticker, self.currency_ticker)

    def test_name_property(self):
        """Test name property is accessible."""
        self.assertIsInstance(self.currency.name, str)
        self.assertGreater(len(self.currency.name), 0)

    def test_country_code_list_property(self):
        """Test country_code_list property is accessible."""
        self.assertIsInstance(self.currency.country_code_list, str)
        self.assertGreater(len(self.currency.country_code_list), 0)





def suite():
    """Create and return test suite with all test classes."""
    test_suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    # Add all test classes whe working
    test_classes = [
        TestCurrency,
    ]

    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    return test_suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
