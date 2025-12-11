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
from src.asset_base.time_series import TimeSeriesBase, ListedEOD, TradeEOD
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
        cls.status = "listed"

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
        self.cash = Cash(self.name, self.currency)
        self.forex = Forex(self.base_currency_ticker, self.price_currency_ticker)
        self.index = Index(self.name, self.currency)
        self.listed_equity = ListedEquity(
            self.listed_name, self.issuer, self.isin, self.exchange,
            self.ticker_symbol, self.status
        )

    def tearDown(self) -> None:
        """Tear down test case fixtures."""
        self.test_session.close()





def suite():
    """Create and return test suite with all test classes."""
    test_suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    # Add all test classes
    test_classes = [
    ]

    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    return test_suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())
