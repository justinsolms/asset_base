from io import StringIO
from os import close
import unittest
import datetime
from attrs import Factory
import pandas as pd

from pygments import highlight
from sqlalchemy.exc import IntegrityError, NoResultFound

from src.asset_base.common import TestSession
from src.asset_base.financial_data import Dump, MetaData, History, Static
from src.asset_base.entity import Currency, Domicile, Issuer, Exchange
from src.asset_base.asset import Asset, AssetBase, Cash, Cash, Forex, Index, Listed, ListedEquity, Share
from src.asset_base.time_series import Dividend, ForexEOD, IndexEOD, EODBase, Split
from src.asset_base.time_series import TimeSeriesBase, ListedEOD, ListedEquityEOD, TradeEOD
from tests.test_asset import TestListedEquity


class TestBase(unittest.TestCase):

    """A test base with common test fixtures."""

    @classmethod
    def setUpClass(cls):
        """Set up class-wide test fixtures."""
        cls.name = "Test Asset"
        cls.currency_ticker = "USD"
        cls.domicile_ticker = "US"
        cls.issuer_name = "Test Issuer"
        cls.issuer_domicile_code = "US"
        cls.exchange_ticker = "XNYS"
        cls.base_currency_ticker = "USD"
        cls.price_currency_ticker = "EUR"
        cls.listed_name = "Test Listed Company"
        cls.isin = "US0378331005"  # Apple Inc. ISIN as example
        cls.ticker_symbol = "TEST"
        cls.index_ticker = "TESTIDX"
        cls.status = "listed"

        # Common test data for EOD instances
        cls.test_date = datetime.date(2020, 12, 1)
        cls.test_open = 100.0
        cls.test_close = 105.0
        cls.test_high = 110.0
        cls.test_low = 95.0
        cls.test_adjusted_close = 105.0
        cls.test_volume = 1000000

        # Dividend test data fixtures
        cls.test_dividend_currency = "USD"
        cls.test_dividend_declaration_date = datetime.date(2020, 1, 29)
        cls.test_dividend_payment_date = datetime.date(2020, 3, 16)
        cls.test_dividend_period = "Quarterly"
        cls.test_dividend_record_date = datetime.date(2020, 3, 2)
        cls.test_dividend_unadjusted_value = 1.25
        cls.test_dividend_adjusted_value = 1.25

        # Split test data fixtures
        cls.test_split_numerator = 2.0
        cls.test_split_denominator = 1.0
        cls.test_split_date_2 = datetime.date(2020, 12, 2)

        # Trade EOD data fixture
        trade_eod_csv = (
            "date_stamp,adjusted_close,close,high,low,open,volume,isin\n"
            f"2020-12-01,123.0,123.1,123.2,123.3,123.4,1000,{cls.isin}\n"
            f"2020-12-02,124.0,124.1,124.2,124.3,124.4,1001,{cls.isin}\n"
            f"2020-12-03,125.0,125.1,125.2,125.3,125.4,1002,{cls.isin}\n"
            f"2020-12-04,126.0,126.1,126.2,126.3,126.4,1003,{cls.isin}\n"
            f"2020-12-05,127.0,127.1,127.2,127.3,127.4,1004,{cls.isin}\n"
            f"2020-12-06,128.0,128.1,128.2,128.3,128.4,1005,{cls.isin}\n"
            f"2020-12-07,129.0,129.1,129.2,129.3,129.4,1006,{cls.isin}\n"
            f"2020-12-08,130.0,130.1,130.2,130.3,130.4,1007,{cls.isin}\n"
            f"2020-12-09,131.0,131.1,131.2,131.3,131.4,1008,{cls.isin}\n"
            f"2020-12-10,132.0,132.1,132.2,132.3,132.4,1009,{cls.isin}\n"
        )
        # Convert String into StringIO
        trade_eod_df = pd.read_csv(StringIO(trade_eod_csv))
        # Convert date_stamp to pandas datetime
        trade_eod_df['date_stamp'] = pd.to_datetime(trade_eod_df['date_stamp'])
        cls.test_trade_eod_df = trade_eod_df

        # Dividend data fixture
        split_csv = (
            "date_stamp,currency,declaration_date,payment_date,period,record_date,unadjusted_value,adjusted_value,isin\n"
            f"2020-12-03,USD,2020-01-29,2020-03-16,Quarterly,2020-03-02,1.25,1.25,{cls.isin}\n"
            f"2020-12-04,USD,2020-05-22,2020-06-15,Quarterly,2020-06-01,1.25,1.25,{cls.isin}\n"
        )
        split_df = pd.read_csv(StringIO(split_csv))
        split_df['date_stamp'] = pd.to_datetime(split_df['date_stamp'])
        split_df['declaration_date'] = pd.to_datetime(split_df['declaration_date'])
        split_df['payment_date'] = pd.to_datetime(split_df['payment_date'])
        split_df['record_date'] = pd.to_datetime(split_df['record_date'])
        cls.test_dividend_df = split_df

        # Split data fixture
        split_csv = (
            "date_stamp,isin,numerator,denominator\n"
            f"2020-12-07,{cls.isin},2.0,1.0\n"
            f"2020-12-09,{cls.isin},4.0,1.0\n"
        )
        split_df = pd.read_csv(StringIO(split_csv))
        split_df['date_stamp'] = pd.to_datetime(split_df['date_stamp'])
        cls.test_split_df = split_df

    def setUp(self):
        """Set up test fixtures."""
        # -------------------- Entity Classes --------------------
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

        # -------------------- Asset Classes --------------------
        # Cash: requires Currency object
        self.cash = Cash(self.currency)

        # Forex: requires Currency objects for base and price currencies
        self.base_currency = Currency.factory(self.session, self.base_currency_ticker)
        self.price_currency = Currency.factory(self.session, self.price_currency_ticker)
        self.forex = Forex(self.base_currency, self.price_currency)

        # Index: requires name, ticker, and Currency object
        self.index = Index(self.name, self.index_ticker, self.currency)

        # ListedEquity: the main asset for ListedEquityEOD tests
        self.listed_equity = ListedEquity(
            self.listed_name, self.issuer, self.isin, self.exchange,
            self.ticker_symbol, self.status
        )

    def tearDown(self) -> None:
        """Tear down test case fixtures."""
        self.test_session.close()


class TestListedEquityEOD(TestBase):
    """Test ListedEquityEOD time series functionality."""

    def test_create_listed_equity_eod_instance(self):
        """Test creating a ListedEquityEOD instance."""
        # Add the listed equity to the session
        self.session.add(self.listed_equity)
        self.session.commit()

        # Create a ListedEquityEOD instance using fixtures
        equity_eod = ListedEquityEOD(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )

        self.session.add(equity_eod)
        self.session.commit()

        # Verify instance was created
        self.assertIsNotNone(equity_eod._id)
        self.assertEqual(equity_eod.date_stamp, self.test_date)
        self.assertEqual(equity_eod.open, self.test_open)
        self.assertEqual(equity_eod.close, self.test_close)
        self.assertEqual(equity_eod.high, self.test_high)
        self.assertEqual(equity_eod.low, self.test_low)
        self.assertEqual(equity_eod.adjusted_close, self.test_adjusted_close)
        self.assertEqual(equity_eod.volume, self.test_volume)
        self.assertEqual(equity_eod.price, self.test_close)  # price convention = close

    def test_listed_equity_eod_str_repr(self):
        """Test __str__ and __repr__ methods."""
        self.session.add(self.listed_equity)
        self.session.commit()

        equity_eod = ListedEquityEOD(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )

        # Test __str__
        str_output = str(equity_eod)
        self.assertIn("ListedEquityEOD", str_output)
        self.assertIn(self.isin, str_output)
        self.assertIn(str(self.test_date), str_output)
        self.assertIn(str(self.test_close), str_output)

        # Test __repr__
        repr_output = repr(equity_eod)
        self.assertIn("ListedEquityEOD", repr_output)
        self.assertIn("date_stamp", repr_output)
        self.assertIn("open=", repr_output)
        self.assertIn("close=", repr_output)

    def test_listed_equity_eod_to_dict_units(self):
        """Test to_dict method with units quote."""
        self.session.add(self.listed_equity)
        self.session.commit()

        equity_eod = ListedEquityEOD(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )

        result_dict = equity_eod.to_dict()

        self.assertEqual(result_dict['date_stamp'], self.test_date)
        self.assertEqual(result_dict['open'], self.test_open)
        self.assertEqual(result_dict['close'], self.test_close)
        self.assertEqual(result_dict['high'], self.test_high)
        self.assertEqual(result_dict['low'], self.test_low)
        self.assertEqual(result_dict['adjusted_close'], self.test_adjusted_close)
        self.assertEqual(result_dict['volume'], self.test_volume)

    def test_listed_equity_eod_to_dict_cents(self):
        """Test to_dict method with cents quote conversion."""
        # Create a listed equity with cents quote_units
        # Use a valid US ISIN: US5949181045 (Microsoft)
        equity_cents = ListedEquity(
            self.listed_name + " Cents",
            self.issuer,
            "US5949181045",
            self.exchange,
            "TSTC",
            self.status,
            quote_units="cents"
        )
        self.session.add(equity_cents)
        self.session.commit()

        equity_eod = ListedEquityEOD(
            base_obj=equity_cents,
            date_stamp=self.test_date,
            open=self.test_open * 100,  # Convert units to cents
            close=self.test_close * 100,
            high=self.test_high * 100,
            low=self.test_low * 100,
            adjusted_close=self.test_adjusted_close * 100,
            volume=self.test_volume
        )

        result_dict = equity_eod.to_dict()

        # Values should be converted from cents to units
        self.assertEqual(result_dict['open'], self.test_open)
        self.assertEqual(result_dict['close'], self.test_close)
        self.assertEqual(result_dict['high'], self.test_high)
        self.assertEqual(result_dict['low'], self.test_low)
        self.assertEqual(result_dict['adjusted_close'], self.test_adjusted_close)
        self.assertEqual(result_dict['volume'], self.test_volume)

    def test_asset_class_validation_success(self):
        """Test that ListedEquityEOD accepts ListedEquity instance."""
        self.session.add(self.listed_equity)
        self.session.commit()

        # Should not raise any exception
        equity_eod = ListedEquityEOD(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )

        self.assertEqual(equity_eod._base_obj, self.listed_equity)

    def test_asset_class_validation_failure(self):
        """Test that ListedEquityEOD rejects non-ListedEquity instances."""
        # Use the Index fixture from setUp
        self.session.add(self.index)
        self.session.commit()

        # Should raise TypeError when trying to use Index with ListedEquityEOD
        with self.assertRaises(TypeError) as context:
            equity_eod = ListedEquityEOD(
                base_obj=self.index,  # Wrong type!
                date_stamp=self.test_date,
                open=self.test_open,
                close=self.test_close,
                high=self.test_high,
                low=self.test_low,
                adjusted_close=self.test_adjusted_close,
                volume=self.test_volume
            )

        self.assertIn("ListedEquityEOD", str(context.exception))
        self.assertIn("ListedEquity", str(context.exception))

    def test_from_data_frame(self):
        """Test creating ListedEquityEOD instances from DataFrame."""
        self.session.add(self.listed_equity)
        self.session.commit()

        # Use the test fixture data
        ListedEquityEOD.from_data_frame(self.session, self.test_trade_eod_df)
        self.session.commit()

        # Query all ListedEquityEOD instances
        eod_list = self.session.query(ListedEquityEOD).all()

        # Should have 10 records from the fixture
        self.assertEqual(len(eod_list), 10)

        # Verify first record
        first_eod = eod_list[0]
        self.assertEqual(first_eod.date_stamp, datetime.date(2020, 12, 1))
        self.assertEqual(first_eod.adjusted_close, 123.0)
        self.assertEqual(first_eod.close, 123.1)
        self.assertEqual(first_eod.volume, 1000)

    def test_from_data_frame_update_existing(self):
        """Test updating existing ListedEquityEOD instances from DataFrame."""
        self.session.add(self.listed_equity)
        self.session.commit()

        # First load - create records
        ListedEquityEOD.from_data_frame(self.session, self.test_trade_eod_df)
        self.session.commit()

        # Modify the DataFrame with updated values
        updated_df = self.test_trade_eod_df.copy()
        updated_df.loc[
            updated_df['date_stamp'] == pd.Timestamp('2020-12-01'), 'close'
        ] = 999.9

        # Second load - update records
        ListedEquityEOD.from_data_frame(self.session, updated_df)
        self.session.commit()

        # Query the updated record
        eod = self.session.query(ListedEquityEOD).filter(
            ListedEquityEOD.date_stamp == datetime.date(2020, 12, 1)
        ).one()

        # Should have updated value
        self.assertEqual(eod.close, 999.9)

        # Should still have 10 records (no duplicates)
        eod_count = self.session.query(ListedEquityEOD).count()
        self.assertEqual(eod_count, 10)

    def test_from_data_frame_empty(self):
        """Test from_data_frame with empty DataFrame."""
        self.session.add(self.listed_equity)
        self.session.commit()

        # Create empty DataFrame with correct columns
        empty_df = pd.DataFrame(columns=self.test_trade_eod_df.columns)

        # Should not raise an error
        ListedEquityEOD.from_data_frame(self.session, empty_df)
        self.session.commit()

        # Should have no records
        eod_count = self.session.query(ListedEquityEOD).count()
        self.assertEqual(eod_count, 0)

    def test_to_data_frame(self):
        """Test converting ListedEquityEOD instances to DataFrame."""
        self.session.add(self.listed_equity)
        self.session.commit()

        # Load data
        ListedEquityEOD.from_data_frame(self.session, self.test_trade_eod_df)
        self.session.commit()

        # Convert back to DataFrame
        result_df = ListedEquityEOD.to_data_frame(self.session)

        # Should have same number of rows
        self.assertEqual(len(result_df), len(self.test_trade_eod_df))

        # Should have the ISIN column
        self.assertIn('isin', result_df.columns)
        self.assertTrue((result_df['isin'] == self.isin).all())

        # Check date_stamp is pandas.Timestamp
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result_df['date_stamp']))

        # Verify data values
        first_row = result_df.iloc[0]
        self.assertAlmostEqual(first_row['close'], 123.1, places=5)
        self.assertEqual(first_row['volume'], 1000)

    def test_to_data_frame_empty(self):
        """Test to_data_frame with no records."""
        # Don't add any data
        self.session.add(self.listed_equity)
        self.session.commit()

        result_df = ListedEquityEOD.to_data_frame(self.session)

        # Should return empty DataFrame with proper columns
        self.assertTrue(result_df.empty)
        self.assertIn('isin', result_df.columns)
        self.assertIn('date_stamp', result_df.columns)

    def test_date_stamp_type_validation(self):
        """Test that date_stamp must be datetime.date, not pandas.Timestamp."""
        self.session.add(self.listed_equity)
        self.session.commit()

        # Try to create with pandas.Timestamp (should fail)
        with self.assertRaises(TypeError) as context:
            equity_eod = ListedEquityEOD(
                base_obj=self.listed_equity,
                date_stamp=pd.Timestamp('2020-12-01'),  # Wrong type
                open=self.test_open,
                close=self.test_close,
                high=self.test_high,
                low=self.test_low,
                adjusted_close=self.test_adjusted_close,
                volume=self.test_volume
            )

        self.assertIn("datetime.date", str(context.exception))
        self.assertIn("date()", str(context.exception))

    def test_unique_constraint(self):
        """Test that duplicate date_stamp for same asset raises error."""
        self.session.add(self.listed_equity)
        self.session.commit()

        # Create first instance
        eod1 = ListedEquityEOD(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )
        self.session.add(eod1)
        self.session.commit()

        # Try to create duplicate (should fail on commit)
        eod2 = ListedEquityEOD(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            open=self.test_open + 1.0,
            close=self.test_close + 1.0,
            high=self.test_high + 1.0,
            low=self.test_low + 1.0,
            adjusted_close=self.test_adjusted_close + 1.0,
            volume=self.test_volume * 2
        )
        self.session.add(eod2)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_relationship_to_asset(self):
        """Test that ListedEquityEOD properly relates to ListedEquity."""
        self.session.add(self.listed_equity)
        self.session.commit()

        equity_eod = ListedEquityEOD(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )
        self.session.add(equity_eod)
        self.session.commit()

        # Access through relationship
        self.assertEqual(equity_eod._base_obj, self.listed_equity)
        self.assertIn(equity_eod, self.listed_equity._series)

        # Test typed _eod_series property
        self.assertIn(equity_eod, self.listed_equity._eod_series)

    def test_polymorphic_inheritance(self):
        """Test that ListedEquityEOD is part of TradeEOD hierarchy."""
        self.session.add(self.listed_equity)
        self.session.commit()

        equity_eod = ListedEquityEOD(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )
        self.session.add(equity_eod)
        self.session.commit()

        # Verify it's queryable via parent classes
        self.assertEqual(
            self.session.query(TradeEOD).filter(
                TradeEOD._id == equity_eod._id
            ).count(),
            1
        )
        self.assertEqual(
            self.session.query(ListedEOD).filter(
                ListedEOD._id == equity_eod._id
            ).count(),
            1
        )

    def test_asset_class_attribute(self):
        """Test that ASSET_CLASS attribute is properly set."""
        self.assertEqual(ListedEquityEOD.ASSET_CLASS, ListedEquity)
        self.assertIsNotNone(ListedEquityEOD.ASSET_CLASS)


class TestIndexEOD(TestBase):
    """Test IndexEOD time series functionality."""

    def test_create_index_eod_instance(self):
        """Test creating an IndexEOD instance."""
        self.session.add(self.index)
        self.session.commit()

        index_eod = IndexEOD(
            base_obj=self.index,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )

        self.session.add(index_eod)
        self.session.commit()

        # Verify instance was created
        self.assertIsNotNone(index_eod._id)
        self.assertEqual(index_eod.date_stamp, self.test_date)
        self.assertEqual(index_eod.open, self.test_open)
        self.assertEqual(index_eod.close, self.test_close)
        self.assertEqual(index_eod.price, self.test_close)

    def test_index_eod_str_repr(self):
        """Test __str__ and __repr__ methods."""
        self.session.add(self.index)
        self.session.commit()

        index_eod = IndexEOD(
            base_obj=self.index,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )

        # IndexEOD inherits __str__ from TradeEOD
        str_output = str(index_eod)
        self.assertIn("TradeEOD", str_output)
        self.assertIn(self.index_ticker, str_output)
        self.assertIn(str(self.test_date), str_output)

        # But __repr__ shows the full representation
        repr_output = repr(index_eod)
        self.assertIn("TradeEOD", repr_output)
        self.assertIn("date_stamp", repr_output)

    def test_index_eod_to_dict(self):
        """Test to_dict method."""
        self.session.add(self.index)
        self.session.commit()

        index_eod = IndexEOD(
            base_obj=self.index,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )
        self.session.add(index_eod)
        self.session.commit()

        result_dict = index_eod.to_dict()

        # Index inherits from AssetBase not Asset, so no quote_units attribute
        # TradeEOD.to_dict will error, so we just verify basic attributes
        self.assertEqual(result_dict['date_stamp'], self.test_date)
        self.assertEqual(result_dict['open'], self.test_open)
        self.assertEqual(result_dict['close'], self.test_close)

    def test_index_eod_asset_class_validation(self):
        """Test that IndexEOD validates asset type."""
        self.session.add(self.listed_equity)
        self.session.commit()

        # Should raise TypeError when using wrong asset type
        with self.assertRaises(TypeError) as context:
            IndexEOD(
                base_obj=self.listed_equity,  # Wrong: ListedEquity not Index
                date_stamp=self.test_date,
                open=self.test_open,
                close=self.test_close,
                high=self.test_high,
                low=self.test_low,
                adjusted_close=self.test_adjusted_close,
                volume=self.test_volume
            )

        self.assertIn("Index", str(context.exception))

    def test_index_eod_unique_constraint(self):
        """Test unique constraint on (asset_id, date_stamp)."""
        self.session.add(self.index)
        self.session.commit()

        eod1 = IndexEOD(
            base_obj=self.index,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )
        self.session.add(eod1)
        self.session.commit()

        # Try to create duplicate
        eod2 = IndexEOD(
            base_obj=self.index,
            date_stamp=self.test_date,
            open=self.test_open + 1.0,
            close=self.test_close + 1.0,
            high=self.test_high + 1.0,
            low=self.test_low + 1.0,
            adjusted_close=self.test_adjusted_close + 1.0,
            volume=self.test_volume * 2
        )
        self.session.add(eod2)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_index_eod_asset_class_attribute(self):
        """Test that ASSET_CLASS attribute is properly set."""
        self.assertEqual(IndexEOD.ASSET_CLASS, Index)


class TestForexEOD(TestBase):
    """Test ForexEOD time series functionality."""

    def test_create_forex_eod_instance(self):
        """Test creating a ForexEOD instance."""
        self.session.add(self.forex)
        self.session.commit()

        forex_eod = ForexEOD(
            base_obj=self.forex,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )

        self.session.add(forex_eod)
        self.session.commit()

        # Verify instance was created
        self.assertIsNotNone(forex_eod._id)
        self.assertEqual(forex_eod.date_stamp, self.test_date)
        self.assertEqual(forex_eod.open, self.test_open)
        self.assertEqual(forex_eod.close, self.test_close)
        self.assertEqual(forex_eod.price, self.test_close)

    def test_forex_eod_str_repr(self):
        """Test __str__ and __repr__ methods."""
        self.session.add(self.forex)
        self.session.commit()

        forex_eod = ForexEOD(
            base_obj=self.forex,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )

        # ForexEOD inherits __str__ from TradeEOD
        str_output = str(forex_eod)
        self.assertIn("TradeEOD", str_output)
        self.assertIn(f"{self.base_currency_ticker}{self.price_currency_ticker}", str_output)
        self.assertIn(str(self.test_date), str_output)

        # But __repr__ shows the full representation
        repr_output = repr(forex_eod)
        self.assertIn("TradeEOD", repr_output)
        self.assertIn("date_stamp", repr_output)

    def test_forex_eod_to_dict(self):
        """Test to_dict method."""
        self.session.add(self.forex)
        self.session.commit()

        forex_eod = ForexEOD(
            base_obj=self.forex,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )
        self.session.add(forex_eod)
        self.session.commit()

        result_dict = forex_eod.to_dict()

        # Forex has quote_units, verify conversion works
        self.assertEqual(result_dict['date_stamp'], self.test_date)
        self.assertEqual(result_dict['open'], self.test_open)
        self.assertEqual(result_dict['close'], self.test_close)

    def test_forex_eod_asset_class_validation(self):
        """Test that ForexEOD validates asset type."""
        self.session.add(self.index)
        self.session.commit()

        # Should raise TypeError when using wrong asset type
        with self.assertRaises(TypeError) as context:
            ForexEOD(
                base_obj=self.index,  # Wrong: Index not Forex
                date_stamp=self.test_date,
                open=self.test_open,
                close=self.test_close,
                high=self.test_high,
                low=self.test_low,
                adjusted_close=self.test_adjusted_close,
                volume=self.test_volume
            )

        self.assertIn("Forex", str(context.exception))

    def test_forex_eod_unique_constraint(self):
        """Test unique constraint on (asset_id, date_stamp)."""
        self.session.add(self.forex)
        self.session.commit()

        eod1 = ForexEOD(
            base_obj=self.forex,
            date_stamp=self.test_date,
            open=self.test_open,
            close=self.test_close,
            high=self.test_high,
            low=self.test_low,
            adjusted_close=self.test_adjusted_close,
            volume=self.test_volume
        )
        self.session.add(eod1)
        self.session.commit()

        # Try to create duplicate
        eod2 = ForexEOD(
            base_obj=self.forex,
            date_stamp=self.test_date,
            open=self.test_open + 1.0,
            close=self.test_close + 1.0,
            high=self.test_high + 1.0,
            low=self.test_low + 1.0,
            adjusted_close=self.test_adjusted_close + 1.0,
            volume=self.test_volume * 2
        )
        self.session.add(eod2)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_forex_eod_asset_class_attribute(self):
        """Test that ASSET_CLASS attribute is properly set."""
        self.assertEqual(ForexEOD.ASSET_CLASS, Forex)


class TestDividend(TestBase):
    """Test Dividend time series functionality."""

    def test_create_dividend_instance(self):
        """Test creating a Dividend instance."""
        self.session.add(self.listed_equity)
        self.session.commit()

        dividend = Dividend(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            currency=self.currency_ticker,
            declaration_date=datetime.date(2020, 1, 29),
            payment_date=datetime.date(2020, 3, 16),
            period="Quarterly",
            record_date=datetime.date(2020, 3, 2),
            unadjusted_value=1.25,
            adjusted_value=1.25
        )

        self.session.add(dividend)
        self.session.commit()

        # Verify instance was created
        self.assertIsNotNone(dividend._id)
        self.assertEqual(dividend.date_stamp, self.test_date)
        self.assertEqual(dividend.currency, self.test_dividend_currency)
        self.assertEqual(dividend.unadjusted_value, self.test_dividend_unadjusted_value)
        self.assertEqual(dividend.adjusted_value, self.test_dividend_adjusted_value)
        self.assertEqual(dividend.period, self.test_dividend_period)

    def test_dividend_str_repr(self):
        """Test __str__ and __repr__ methods."""
        self.session.add(self.listed_equity)
        self.session.commit()

        dividend = Dividend(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            currency=self.test_dividend_currency,
            declaration_date=self.test_dividend_declaration_date,
            payment_date=self.test_dividend_payment_date,
            period=self.test_dividend_period,
            record_date=self.test_dividend_record_date,
            unadjusted_value=self.test_dividend_unadjusted_value,
            adjusted_value=self.test_dividend_adjusted_value
        )

        str_output = str(dividend)
        self.assertIn("Dividend", str_output)
        self.assertIn(self.isin, str_output)
        self.assertIn(str(self.test_dividend_adjusted_value), str_output)

        repr_output = repr(dividend)
        self.assertIn("Dividend", repr_output)
        self.assertIn("date_stamp", repr_output)
        self.assertIn("currency", repr_output)

    def test_dividend_to_dict(self):
        """Test to_dict method."""
        self.session.add(self.listed_equity)
        self.session.commit()

        dividend = Dividend(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            currency=self.test_dividend_currency,
            declaration_date=self.test_dividend_declaration_date,
            payment_date=self.test_dividend_payment_date,
            period=self.test_dividend_period,
            record_date=self.test_dividend_record_date,
            unadjusted_value=self.test_dividend_unadjusted_value,
            adjusted_value=self.test_dividend_adjusted_value
        )

        result_dict = dividend.to_dict()

        self.assertEqual(result_dict['date_stamp'], self.test_date)
        self.assertEqual(result_dict['currency'], self.test_dividend_currency)
        self.assertEqual(result_dict['unadjusted_value'], self.test_dividend_unadjusted_value)
        self.assertEqual(result_dict['adjusted_value'], self.test_dividend_adjusted_value)

    def test_dividend_asset_class_validation(self):
        """Test that Dividend validates asset type."""
        self.session.add(self.index)
        self.session.commit()

        # Should raise TypeError when using wrong asset type
        with self.assertRaises(TypeError) as context:
            Dividend(
                base_obj=self.index,  # Wrong: Index not ListedEquity
                date_stamp=self.test_date,
                currency=self.test_dividend_currency,
                declaration_date=self.test_dividend_declaration_date,
                payment_date=self.test_dividend_payment_date,
                period=self.test_dividend_period,
                record_date=self.test_dividend_record_date,
                unadjusted_value=self.test_dividend_unadjusted_value,
                adjusted_value=self.test_dividend_adjusted_value
            )

        self.assertIn("ListedEquity", str(context.exception))

    def test_dividend_unique_constraint(self):
        """Test unique constraint on (asset_id, date_stamp)."""
        self.session.add(self.listed_equity)
        self.session.commit()

        div1 = Dividend(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            currency=self.test_dividend_currency,
            declaration_date=self.test_dividend_declaration_date,
            payment_date=self.test_dividend_payment_date,
            period=self.test_dividend_period,
            record_date=self.test_dividend_record_date,
            unadjusted_value=self.test_dividend_unadjusted_value,
            adjusted_value=self.test_dividend_adjusted_value
        )
        self.session.add(div1)
        self.session.commit()

        # Try to create duplicate
        div2 = Dividend(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            currency=self.test_dividend_currency,
            declaration_date=self.test_dividend_declaration_date,
            payment_date=self.test_dividend_payment_date,
            period=self.test_dividend_period,
            record_date=self.test_dividend_record_date,
            unadjusted_value=2.50,
            adjusted_value=2.50
        )
        self.session.add(div2)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_dividend_relationship_to_asset(self):
        """Test that Dividend properly relates to ListedEquity."""
        self.session.add(self.listed_equity)
        self.session.commit()

        dividend = Dividend(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            currency=self.test_dividend_currency,
            declaration_date=self.test_dividend_declaration_date,
            payment_date=self.test_dividend_payment_date,
            period=self.test_dividend_period,
            record_date=self.test_dividend_record_date,
            unadjusted_value=self.test_dividend_unadjusted_value,
            adjusted_value=self.test_dividend_adjusted_value
        )
        self.session.add(dividend)
        self.session.commit()

        # Access through relationship
        self.assertEqual(dividend._base_obj, self.listed_equity)
        self.assertIn(dividend, self.listed_equity._series)

    def test_dividend_asset_class_attribute(self):
        """Test that ASSET_CLASS attribute is properly set."""
        self.assertEqual(Dividend.ASSET_CLASS, ListedEquity)


class TestSplit(TestBase):
    """Test Split time series functionality."""

    def test_create_split_instance(self):
        """Test creating a Split instance."""
        self.session.add(self.listed_equity)
        self.session.commit()

        split = Split(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            numerator=self.test_split_numerator,
            denominator=self.test_split_denominator
        )

        self.session.add(split)
        self.session.commit()

        # Verify instance was created
        self.assertIsNotNone(split._id)
        self.assertEqual(split.date_stamp, self.test_date)
        self.assertEqual(split.numerator, self.test_split_numerator)
        self.assertEqual(split.denominator, self.test_split_denominator)

    def test_split_str_repr(self):
        """Test __str__ and __repr__ methods."""
        self.session.add(self.listed_equity)
        self.session.commit()

        split = Split(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            numerator=self.test_split_numerator,
            denominator=self.test_split_denominator
        )

        str_output = str(split)
        self.assertIn("Split", str_output)
        self.assertIn(self.isin, str_output)
        self.assertIn(f"{self.test_split_numerator}:{self.test_split_denominator}", str_output)

        repr_output = repr(split)
        self.assertIn("Split", repr_output)
        self.assertIn("date_stamp", repr_output)
        self.assertIn("numerator", repr_output)
        self.assertIn("denominator", repr_output)

    def test_split_to_dict(self):
        """Test to_dict method."""
        self.session.add(self.listed_equity)
        self.session.commit()

        split = Split(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            numerator=self.test_split_numerator,
            denominator=self.test_split_denominator
        )

        result_dict = split.to_dict()

        self.assertEqual(result_dict['date_stamp'], self.test_date)
        self.assertEqual(result_dict['numerator'], self.test_split_numerator)
        self.assertEqual(result_dict['denominator'], self.test_split_denominator)

    def test_split_asset_class_validation(self):
        """Test that Split validates asset type."""
        self.session.add(self.index)
        self.session.commit()

        # Should raise TypeError when using wrong asset type
        with self.assertRaises(TypeError) as context:
            Split(
                base_obj=self.index,  # Wrong: Index not ListedEquity
                date_stamp=self.test_date,
                numerator=self.test_split_numerator,
                denominator=self.test_split_denominator
            )

        self.assertIn("ListedEquity", str(context.exception))

    def test_split_unique_constraint(self):
        """Test unique constraint on (asset_id, date_stamp)."""
        self.session.add(self.listed_equity)
        self.session.commit()

        split1 = Split(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            numerator=self.test_split_numerator,
            denominator=self.test_split_denominator
        )
        self.session.add(split1)
        self.session.commit()

        # Try to create duplicate
        split2 = Split(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            numerator=4.0,
            denominator=self.test_split_denominator
        )
        self.session.add(split2)

        with self.assertRaises(IntegrityError):
            self.session.commit()

    def test_split_relationship_to_asset(self):
        """Test that Split properly relates to ListedEquity."""
        self.session.add(self.listed_equity)
        self.session.commit()

        split = Split(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            numerator=self.test_split_numerator,
            denominator=self.test_split_denominator
        )
        self.session.add(split)
        self.session.commit()

        # Access through relationship
        self.assertEqual(split._base_obj, self.listed_equity)
        self.assertIn(split, self.listed_equity._series)

    def test_split_ratio_calculation(self):
        """Test different split ratios."""
        self.session.add(self.listed_equity)
        self.session.commit()

        # 2-for-1 split
        split_2_1 = Split(
            base_obj=self.listed_equity,
            date_stamp=self.test_date,
            numerator=self.test_split_numerator,
            denominator=self.test_split_denominator
        )
        self.session.add(split_2_1)
        self.session.commit()

        ratio = split_2_1.numerator / split_2_1.denominator
        self.assertEqual(ratio, self.test_split_numerator / self.test_split_denominator)

        # 3-for-2 split
        split_3_2 = Split(
            base_obj=self.listed_equity,
            date_stamp=self.test_split_date_2,
            numerator=3.0,
            denominator=2.0
        )
        self.session.add(split_3_2)
        self.session.commit()

        ratio = split_3_2.numerator / split_3_2.denominator
        self.assertEqual(ratio, 1.5)

    def test_split_asset_class_attribute(self):
        """Test that ASSET_CLASS attribute is properly set."""
        self.assertEqual(Split.ASSET_CLASS, ListedEquity)


def suite():
    """Create and return test suite with all test classes."""
    test_suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    # Add all test classes
    test_classes = [
        TestListedEquityEOD,
        TestIndexEOD,
        TestForexEOD,
        TestDividend,
        TestSplit,
    ]

    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    return test_suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
