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
        # Test values for creating new instances
        cls.test_domicile_code = "CA"
        cls.test_domicile_name = "Canada"
        cls.test_currency_ticker = "CAD"
        cls.test_issuer_name = "Test Corporation"
        cls.test_exchange_name = "Test Exchange"
        cls.test_exchange_mic = "TEST"
        cls.test_exchange_eod = "TST"
        # Pre-loaded test values from static files
        cls.preloaded_domicile_gb = "GB"
        cls.preloaded_domicile_fr = "FR"
        cls.preloaded_domicile_de = "DE"
        cls.preloaded_domicile_mx = "MX"
        cls.preloaded_currency_mxn = "MXN"
        cls.preloaded_currency_eur = "EUR"
        # Non-existent values for negative tests
        cls.nonexistent_country = "ZZ"
        cls.nonexistent_mic = "XXXX"
        cls.wrong_name = "Wrong Name"
        cls.wrong_country = "XX"

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


class TestDomicile(TestBase):
    """Test the Domicile class."""

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Use US domicile from TestBase fixtures

    def test_class_initialization(self):
        """Test class initialization."""
        domicile = Domicile(
            country_code=self.test_domicile_code,
            country_name=self.test_domicile_name,
            currency=self.currency
        )
        self.assertIsInstance(domicile, Domicile)
        self.assertEqual(domicile.country_code, self.test_domicile_code)
        self.assertEqual(domicile.country_name, self.test_domicile_name)
        self.assertEqual(domicile.currency, self.currency)

    def test_str_method(self):
        """Test __str__ method returns correct format."""
        result = str(self.domicile)
        self.assertIn("Domicile", result)
        self.assertIn(self.domicile.country_name, result)
        self.assertIn(self.domicile.country_code, result)

    def test_repr_method(self):
        """Test __repr__ method returns correct format."""
        result = repr(self.domicile)
        self.assertIn("Domicile", result)
        self.assertIn(f'country_code="{self.domicile.country_code}"', result)
        self.assertIn(f'country_name="{self.domicile.country_name}"', result)
        self.assertIn("currency=", result)

    def test_key_code_property(self):
        """Test key_code property returns country_code."""
        self.assertEqual(self.domicile.key_code, self.domicile_ticker)

    def test_identity_code_property(self):
        """Test identity_code property returns country_code."""
        self.assertEqual(self.domicile.identity_code, self.domicile_ticker)

    def test_class_name_method(self):
        """Test _class_name method returns class name."""
        self.assertEqual(Domicile._class_name(), "Domicile")

    def test_database_persistence(self):
        """Test that Domicile instance can be persisted to database."""
        retrieved = self.session.query(Domicile).filter(
            Domicile.country_code == self.domicile_ticker
        ).first()
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.country_code, self.domicile_ticker)
        self.assertIsInstance(retrieved.country_name, str)
        self.assertIsInstance(retrieved.currency, Currency)

    def test_unique_country_code_constraint(self):
        """Test that country_code uniqueness is enforced."""
        domicile_duplicate = Domicile(
            country_code=self.domicile_ticker,
            country_name="Different Name",
            currency=self.currency
        )
        self.session.add(domicile_duplicate)

        from sqlalchemy.exc import IntegrityError
        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_currency_relationship(self):
        """Test that currency relationship is properly set."""
        self.assertIsInstance(self.domicile.currency, Currency)
        self.assertEqual(self.domicile.currency.ticker, self.currency_ticker)

    def test_factory_retrieves_existing_instance(self):
        """Test factory method retrieves existing Domicile instance."""
        domicile_instance = Domicile.factory(self.session, country_code=self.domicile_ticker)
        self.assertIsInstance(domicile_instance, Domicile)
        self.assertEqual(domicile_instance.country_code, self.domicile_ticker)

    def test_factory_creates_new_instance(self):
        """Test factory method creates new instance with all params."""
        new_domicile = Domicile.factory(
            self.session,
            country_code=self.preloaded_domicile_mx,
            country_name="Mexico",
            currency_ticker=self.preloaded_currency_mxn
        )
        self.assertIsInstance(new_domicile, Domicile)
        self.assertEqual(new_domicile.country_code, self.preloaded_domicile_mx)
        self.assertEqual(new_domicile.country_name, "Mexico")

    def test_factory_requires_2_letter_country_code(self):
        """Test factory asserts 2-letter country code."""
        with self.assertRaises(AssertionError):
            Domicile.factory(
                self.session,
                country_code="USA",
                country_name="United States",
                currency_ticker="USD"
            )

    def test_factory_raises_error_without_required_params(self):
        """Test factory raises error when creating without required params."""
        with self.assertRaises(FactoryError):
            Domicile.factory(self.session, country_code="XX")

    def test_factory_raises_error_on_country_name_mismatch(self):
        """Test factory raises error when country_name doesn't match existing."""
        with self.assertRaises(FactoryError):
            Domicile.factory(
                self.session,
                country_code=self.domicile_ticker,
                country_name="Wrong Name"
            )

    def test_factory_raises_error_on_currency_mismatch(self):
        """Test factory raises error when currency_ticker doesn't match existing."""
        with self.assertRaises(FactoryError):
            Domicile.factory(
                self.session,
                country_code=self.domicile_ticker,
                currency_ticker=self.preloaded_currency_eur
            )

    def test_from_data_frame(self):
        """Test from_data_frame retrieves domiciles from dataframe."""
        data = pd.DataFrame({
            'country_code': [self.preloaded_domicile_gb, self.preloaded_domicile_fr, self.preloaded_domicile_de],
        })
        Domicile.from_data_frame(self.session, data)
        self.session.commit()

        gb = self.session.query(Domicile).filter(Domicile.country_code == self.preloaded_domicile_gb).first()
        fr = self.session.query(Domicile).filter(Domicile.country_code == self.preloaded_domicile_fr).first()
        de = self.session.query(Domicile).filter(Domicile.country_code == self.preloaded_domicile_de).first()

        self.assertIsNotNone(gb)
        self.assertIsNotNone(fr)
        self.assertIsNotNone(de)

    def test_update_all_creates_domiciles(self):
        """Test update_all creates Domicile instances."""
        count_before = self.session.query(Domicile).count()
        Domicile.update_all(self.session, get_method=Static().get_domicile)
        self.session.commit()
        count_after = self.session.query(Domicile).count()

        self.assertGreater(count_after, 0)
        self.assertEqual(count_before, count_after)

    def test_country_code_property(self):
        """Test country_code property is accessible."""
        self.assertEqual(self.domicile.country_code, self.domicile_ticker)

    def test_country_name_property(self):
        """Test country_name property is accessible."""
        self.assertIsInstance(self.domicile.country_name, str)
        self.assertGreater(len(self.domicile.country_name), 0)

    def test_currency_property(self):
        """Test currency property is accessible."""
        self.assertIsInstance(self.domicile.currency, Currency)


class TestIssuer(TestBase):
    """Test the Issuer class."""

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Use issuer from TestBase fixtures

    def test_class_initialization(self):
        """Test class initialization."""
        issuer = Issuer(name=self.test_issuer_name, domicile=self.domicile)
        self.assertIsInstance(issuer, Issuer)
        self.assertEqual(issuer.name, self.test_issuer_name)
        self.assertEqual(issuer.domicile, self.domicile)

    def test_str_method(self):
        """Test __str__ method returns correct format."""
        result = str(self.issuer)
        self.assertIn(self.issuer.name, result)
        self.assertIn("Issuer", result)
        self.assertIn(self.domicile.country_name, result)

    def test_repr_method(self):
        """Test __repr__ method returns correct format."""
        result = repr(self.issuer)
        self.assertIn("Issuer", result)
        self.assertIn(f'name="{self.issuer.name}"', result)
        self.assertIn("domicile=", result)

    def test_key_code_property(self):
        """Test key_code property returns name:country_code format."""
        expected = f"{self.issuer_name}:{self.issuer_domicile_code}"
        self.assertEqual(self.issuer.key_code, expected)

    def test_identity_code_property(self):
        """Test identity_code property returns name:country_code format."""
        expected = f"{self.issuer_name}:{self.issuer_domicile_code}"
        self.assertEqual(self.issuer.identity_code, expected)

    def test_long_name_property(self):
        """Test long_name property returns name (country_code) format."""
        expected = f"{self.issuer_name} ({self.issuer_domicile_code})"
        self.assertEqual(self.issuer.long_name, expected)

    def test_database_persistence(self):
        """Test that Issuer instance can be persisted to database."""
        retrieved = self.session.query(Issuer).filter(Issuer.name == self.issuer_name).first()
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, self.issuer_name)
        self.assertIsInstance(retrieved.domicile, Domicile)

    def test_domicile_relationship(self):
        """Test that domicile relationship is properly set."""
        self.assertIsInstance(self.issuer.domicile, Domicile)
        self.assertEqual(self.issuer.domicile.country_code, self.issuer_domicile_code)

    def test_currency_property(self):
        """Test that currency property returns domicile currency."""
        self.assertIsInstance(self.issuer.currency, Currency)
        self.assertEqual(self.issuer.currency, self.domicile.currency)

    def test_inheritance_from_entity(self):
        """Test that Issuer inherits from Entity."""
        self.assertIsInstance(self.issuer, Entity)

    def test_factory_creates_instance(self):
        """Test factory method creates new Issuer instance."""
        new_corp_name = "New Corporation"
        issuer = Issuer.factory(
            self.session,
            entity_name=new_corp_name,
            country_code=self.domicile_ticker
        )
        self.assertIsInstance(issuer, Issuer)
        self.assertEqual(issuer.name, new_corp_name)
        self.assertEqual(issuer.domicile.country_code, self.domicile_ticker)

    def test_factory_retrieves_existing_instance(self):
        """Test factory method retrieves existing Issuer instance."""
        issuer1 = Issuer.factory(
            self.session,
            entity_name=self.issuer_name,
            country_code=self.issuer_domicile_code
        )
        issuer2 = Issuer.factory(
            self.session,
            entity_name=self.issuer_name,
            country_code=self.issuer_domicile_code
        )
        self.assertEqual(issuer1._id, issuer2._id)
        self.assertEqual(issuer1.name, issuer2.name)

    def test_date_create_property(self):
        """Test that date_create is set on initialization."""
        self.assertIsNotNone(self.issuer.date_create)
        import datetime
        self.assertIsInstance(self.issuer.date_create, datetime.date)

    def test_date_mod_stamp_property(self):
        """Test that date_mod_stamp is None on initialization."""
        self.assertIsNone(self.issuer.date_mod_stamp)

    def test_to_dict_method(self):
        """Test to_dict method returns factory-compatible dictionary."""
        result = self.issuer.to_dict()
        self.assertIsInstance(result, dict)
        self.assertIn("entity_name", result)
        self.assertIn("country_code", result)
        self.assertEqual(result["entity_name"], self.issuer_name)
        self.assertEqual(result["country_code"], self.issuer_domicile_code)

    def test_name_property(self):
        """Test name property is accessible."""
        self.assertEqual(self.issuer.name, self.issuer_name)

    def test_polymorphic_identity(self):
        """Test that Issuer has correct polymorphic identity."""
        self.assertEqual(self.issuer.__class__.__name__, "Issuer")


class TestExchange(TestBase):
    """Test the Exchange class."""

    def setUp(self):
        """Set up test case fixtures."""
        super().setUp()
        # Use XNYS exchange from TestBase fixtures

    def test_class_initialization(self):
        """Test class initialization."""
        exchange = Exchange(
            name=self.test_exchange_name,
            domicile=self.domicile,
            mic=self.test_exchange_mic
        )
        self.assertIsInstance(exchange, Exchange)
        self.assertEqual(exchange.name, self.test_exchange_name)
        self.assertEqual(exchange.domicile, self.domicile)
        self.assertEqual(exchange.mic, self.test_exchange_mic)

    def test_class_initialization_with_eod_code(self):
        """Test class initialization with optional eod_code."""
        exchange = Exchange(
            name=self.test_exchange_name,
            domicile=self.domicile,
            mic=self.test_exchange_mic,
            eod_code=self.test_exchange_eod
        )
        self.assertEqual(exchange.eod_code, self.test_exchange_eod)

    def test_str_method(self):
        """Test __str__ method returns correct format."""
        result = str(self.exchange)
        self.assertIn(self.exchange.name, result)
        self.assertIn(self.exchange.mic, result)
        self.assertIn("Exchange", result)
        self.assertIn(self.domicile.country_name, result)

    def test_repr_method(self):
        """Test __repr__ method returns correct format."""
        result = repr(self.exchange)
        self.assertIn("Exchange", result)
        self.assertIn(f'name="{self.exchange.name}"', result)
        self.assertIn(f'mic="{self.exchange.mic}"', result)
        self.assertIn("domicile=", result)

    def test_key_code_property(self):
        """Test key_code property returns mic."""
        self.assertEqual(self.exchange.key_code, self.exchange_ticker)

    def test_identity_code_property(self):
        """Test identity_code property returns mic."""
        self.assertEqual(self.exchange.identity_code, self.exchange_ticker)

    def test_long_name_property(self):
        """Test long_name property returns name (mic) format."""
        expected = f"{self.exchange.name} ({self.exchange_ticker})"
        self.assertEqual(self.exchange.long_name, expected)

    def test_key_code_label_constant(self):
        """Test KEY_CODE_LABEL class constant."""
        self.assertEqual(Exchange.KEY_CODE_LABEL, "mic")

    def test_database_persistence(self):
        """Test that Exchange instance can be persisted to database."""
        retrieved = self.session.query(Exchange).filter(Exchange.mic == self.exchange_ticker).first()
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.mic, self.exchange_ticker)
        self.assertIsInstance(retrieved.domicile, Domicile)

    def test_domicile_relationship(self):
        """Test that domicile relationship is properly set."""
        self.assertIsInstance(self.exchange.domicile, Domicile)

    def test_currency_property(self):
        """Test that currency property returns domicile currency."""
        self.assertIsInstance(self.exchange.currency, Currency)

    def test_inheritance_from_entity(self):
        """Test that Exchange inherits from Entity."""
        self.assertIsInstance(self.exchange, Entity)

    def test_factory_retrieves_by_mic(self):
        """Test factory method retrieves existing Exchange by mic."""
        exchange = Exchange.factory(self.session, mic=self.exchange_ticker)
        self.assertIsInstance(exchange, Exchange)
        self.assertEqual(exchange.mic, self.exchange_ticker)

    def test_factory_creates_new_instance(self):
        """Test factory method creates new Exchange instance."""
        exchange = Exchange.factory(
            self.session,
            mic=self.test_exchange_mic,
            exchange_name=self.test_exchange_name,
            country_code=self.domicile_ticker
        )
        self.assertIsInstance(exchange, Exchange)
        self.assertEqual(exchange.mic, self.test_exchange_mic)
        self.assertEqual(exchange.name, self.test_exchange_name)

    def test_factory_creates_with_eod_code(self):
        """Test factory method creates instance with eod_code."""
        test_eod_mic = "TEOD"
        test_eod_name = "Test EOD Exchange"
        exchange = Exchange.factory(
            self.session,
            mic=test_eod_mic,
            exchange_name=test_eod_name,
            country_code=self.domicile_ticker,
            eod_code=test_eod_mic
        )
        self.assertEqual(exchange.eod_code, test_eod_mic)

    def test_factory_retrieves_existing_instance(self):
        """Test factory method retrieves existing Exchange instance."""
        exchange1 = Exchange.factory(self.session, mic=self.exchange_ticker)
        exchange2 = Exchange.factory(self.session, mic=self.exchange_ticker)
        self.assertEqual(exchange1._id, exchange2._id)
        self.assertEqual(exchange1.mic, exchange2.mic)

    def test_factory_raises_error_without_create_params(self):
        """Test factory raises error when creating without required params."""
        with self.assertRaises(FactoryError):
            Exchange.factory(self.session, mic=self.nonexistent_mic)

    def test_factory_raises_error_on_country_code_mismatch(self):
        """Test factory raises ReconcileError when country_code doesn't match."""
        with self.assertRaises(ReconcileError):
            Exchange.factory(
                self.session,
                mic=self.exchange_ticker,
                country_code=self.wrong_country
            )

    def test_factory_raises_error_on_name_mismatch(self):
        """Test factory raises ReconcileError when exchange_name doesn't match."""
        with self.assertRaises(ReconcileError):
            Exchange.factory(
                self.session,
                mic=self.exchange_ticker,
                exchange_name=self.wrong_name
            )

    def test_factory_with_create_false_retrieves_existing(self):
        """Test factory with create=False retrieves existing instance."""
        exchange = Exchange.factory(
            self.session,
            mic=self.exchange_ticker,
            create=False
        )
        self.assertIsInstance(exchange, Exchange)
        self.assertEqual(exchange.mic, self.exchange_ticker)

    def test_factory_with_create_false_raises_error_for_missing(self):
        """Test factory with create=False raises error for missing instance."""
        with self.assertRaises(FactoryError):
            Exchange.factory(
                self.session,
                mic=self.nonexistent_mic,
                create=False
            )

    def test_update_all_creates_exchanges(self):
        """Test update_all creates Exchange instances."""
        count_before = self.session.query(Exchange).count()
        Exchange.update_all(self.session, get_method=Static().get_exchange)
        self.session.commit()
        count_after = self.session.query(Exchange).count()

        self.assertGreater(count_after, 0)
        self.assertEqual(count_before, count_after)

    def test_mic_property(self):
        """Test mic property is accessible."""
        self.assertEqual(self.exchange.mic, self.exchange_ticker)

    def test_name_property(self):
        """Test name property is accessible."""
        self.assertIsInstance(self.exchange.name, str)
        self.assertGreater(len(self.exchange.name), 0)

    def test_date_create_property(self):
        """Test that date_create is set on initialization."""
        self.assertIsNotNone(self.exchange.date_create)
        import datetime
        self.assertIsInstance(self.exchange.date_create, datetime.date)


def suite():
    """Create and return test suite with all test classes."""
    test_suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    # Add all test classes
    test_classes = [
        TestCurrency,
        TestDomicile,
        TestIssuer,
        TestExchange,
    ]

    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    return test_suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
