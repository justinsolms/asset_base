#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Define classes describing assets such as financial assets.

This module defines asset classes representing financial instruments including
cash, forex pairs, shares, listed equities, and exchange-traded funds.

Factory Method Paradigm
------------------------
All asset classes in this module implement the factory method pattern with
dual-mode behaviour, similar to entity classes:

**Retrieval Mode** (minimal parameters):
    When only key identifying parameters are provided, the factory attempts
    to retrieve an existing instance. If not found, raises ``FactoryError``.

**Creation Mode** (full parameters):
    When all required parameters are provided, the factory retrieves an
    existing instance if found, or creates a new one if missing.

**Dependency on Entity Module**:
    Asset factories depend on entities (Currency, Domicile, Issuer, Exchange)
    being pre-loaded. Asset factories call entity factories in retrieval mode
    to enforce that these dependencies must exist:

    Examples::

        # Cash.factory calls Currency.factory in retrieval mode
        cash = Cash.factory(session, currency_ticker="USD")
        # Raises FactoryError if USD currency doesn't exist

        # Forex.factory calls Currency.factory for both currencies
        forex = Forex.factory(
            session, base_currency_ticker="USD",
            price_currency_ticker="EUR"
        )
        # Raises FactoryError if either currency doesn't exist

        # Listed.factory calls Issuer.factory and Exchange.factory
        listed = Listed.factory(
            session, isin="US0378331005",
            issuer_name="Apple Inc", issuer_country_code="US",
            exchange_mic="XNYS", ticker="AAPL"
        )
        # Raises FactoryError if issuer domicile or exchange doesn't exist

**Typical Usage Pattern**:
    1. Load foundational data (Currency, Domicile) using ``update_all()``
    2. Load entity data (Issuer, Exchange) using ``update_all()``
    3. Create or load assets, which reference pre-existing entities

    Example::

        # Step 1: Load currencies and domiciles
        Currency.update_all(session, get_currency_data)
        Domicile.update_all(session, get_domicile_data)

        # Step 2: Load exchanges and issuers
        Exchange.update_all(session, get_exchange_data)
        Issuer.update_all(session, get_issuer_data)

        # Step 3: Now safe to create assets
        listed = Listed.factory(
            session, isin="US0378331005",
            issuer_name="Apple Inc", issuer_country_code="US",
            exchange_mic="XNYS", ticker="AAPL",
            quote_units="units"
        )

See Also
--------
entity : Entity classes that assets depend on
common : Base Common class and factory pattern documentation
"""

from typing import ClassVar

import sys
import functools
from flask.cli import F
import numpy as np
import pandas as pd

import stdnum.isin as stdisin

from numpy import abs, divide
from scipy.signal import filtfilt

from sqlalchemy import Float, Integer, String, Enum, Boolean, UniqueConstraint, column
from sqlalchemy import MetaData, Column, ForeignKey

from sqlalchemy.orm import foreign, relationship
from sqlalchemy.orm import object_session
from sqlalchemy.orm.exc import NoResultFound
from zmq import METADATA

from asset_base.exceptions import FactoryError, EODSeriesNoData, DividendSeriesNoData, SplitSeriesNoData
from asset_base.exceptions import ReconcileError
from asset_base.exceptions import BadISIN
from asset_base.financial_data import Dump
from asset_base.entity import Currency, Exchange, Issuer
from asset_base.common import Common
from asset_base.industry_class import IndustryClassICB
from asset_base.time_series import TimeSeriesBase, EODBase
from asset_base.time_series import Dividend, Split
from asset_base.time_series import ListedEOD, ListedEquityEOD, ForexEOD, IndexEOD

from asset_base.time_series_processor import TimeSeriesProcessor

from asset_base.financial_data import History as FinancialHistory

# Get module-named logger.
import logging

logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Pull in the meta data
metadata = MetaData()

ASSET_CLASSES = ("money", "bond", "property", "equity", "commodity", "multi")

@functools.total_ordering
class AssetBase(Common):
    """Base class for the module.

    Parameters
    ----------
    name : str
        Entity full name.
    currency : .entity.Currency
        Currency of asset pricing.
    quote_units : {'units', 'cents'}, optional
        Price quotations are either in currency units (default) or currency
        cents.

    Note
    ----
    This class uses the ``@functools.total_ordering`` decorator and implements
    ``__lt__`` ("<" operator) overloading so that when these class instances are
    used in a list or other ordered collection they are sorted by their ``id``.
    A critical application example is when using class instances in a
    ``pandas.MultiIndex`` and applying a ``groupby`` operation we avoid the
    `TypeError: '<' not supported between instances of <Base polymorph> and
    <other Base polymorph>` error.
    """

    __tablename__ = "asset_base"
    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
    }

    _id = Column(Integer, ForeignKey("common._id"), primary_key=True)
    """ Primary key."""

    # Each Asset has one Currency.
    _currency_id = Column(Integer, ForeignKey("currency._id"), nullable=False)
    currency = relationship(Currency)

    # Price quote in cents or units. Strictly convert all prices to currency
    # units in case of this attribute being in cents.
    quote_units = Column(Enum("units", "cents"), nullable=False)

    # The financial_data module history getter method provider that will
    # populate the _time_series_single_item relationship for this class.
    HISTORY_INSTANCE = FinancialHistory()

    # The financial_data EOD_GET_METHOD method overridden here
    EOD_GET_METHOD = HISTORY_INSTANCE.get_trade_eod

    # Associated time-series class for this asset class. This must be overridden
    # in child classes.
    TIME_SERIES_CLASS = EODBase

    # All historical generic time-series collection ranked by date_stamp
    _time_series_single_item = relationship(
        TimeSeriesBase,
        order_by=TimeSeriesBase.date_stamp,
        back_populates="_base_obj",
        uselist=True,
        lazy='selectin',
        )

    def __init__(self, name, currency, quote_units="units"):
        """Instance initialization."""
        self.currency = currency

        # Check quote_units is valid
        if quote_units not in ("units", "cents"):
            raise ValueError(
                f"Unexpected `quote_units` argument {quote_units}. "
                "Expected 'units' or 'cents'.")
        self.quote_units = quote_units

        super().__init__(name)

    def __lt__(self, other):
        """Use primarily key ``id`` for sorting. (See Note in class docstring)."""
        return self._id < other._id

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return self.identity_code

    @property
    def currency_ticker(self):
        """ISO 4217 3-letter currency code."""
        return self.currency.ticker

    @classmethod
    def update_meta_data(cls, session):
        """Update/create instances of the class.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.

        """
        # Bulk add/update metadata uses the class factory method
        data_frame = cls.METADATA_GET_METHOD()
        cls.from_data_frame(session, data_frame)

    @classmethod
    def update_eod_time_series(cls, session, asset_list):
        """Update/create the EOD data of all the Asset or child class instances.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        asset_list : list of Asset or child class instances
            The list of asset instances to update the EOD time-series for.

        """
        # Bulk add/update time-series data uses the time-series class factory
        # method.
        data_frame = cls.EOD_GET_METHOD(asset_list)
        cls.TIME_SERIES_CLASS.from_data_frame(session, cls, data_frame)

    @classmethod
    def update_all(cls, session):
        """Update/create Listed instances and their trade time-series data.

        Updates time series for only listed securities ignoring de-listed ones.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.

        Note
        ----
        It will often be necessary to override this method in child classes
        and it is possible that this instance of this method mey never be used.

        """
        # Update Listed instances metadata as per the latest data from the data source.
        cls.update_meta_data(session)

        # Only update time series for instance in the database.
        asset_list = session.query(cls).all()

        # Get EOD trade data for this Listed subclass.
        cls.update_eod_time_series(session, asset_list)

class Asset(AssetBase):
    """A financial asset.

    Note
    ----
    This is an abstract class not meant for direct instantiation.

    In financial accounting, an asset is an economic resource. Anything
    tangible or intangible that is capable of being owned or controlled to
    produce value and that is held to have positive economic value is
    considered an asset. Simply stated, assets represent value of ownership
    that can be converted into cash (although cash itself is also considered an
    asset) - Wikipedia

    Investopedia defines asset as:

    1. A resource with economic value that an individual, corporation or
       country owns or controls with the expectation that it will provide
       future benefit.
    2. A balance sheet item representing what a firm owns.

    Investopedia explains asset:

    1. Assets are bought to increase the value of a firm or benefit the firm's
       operations. You can think of an asset as something that can generate
       cash flow, regardless of whether it's a company's manufacturing
       equipment or an individual's rental apartment.
    2. In the context of accounting, assets are either current or fixed
       (non-current). Current means that the asset will be consumed within one
       year. Generally, this includes things like cash, accounts receivable and
       inventory. Fixed assets are those that are expected to keep providing
       benefit for more than one year, such as equipment, buildings and real
       estate.

    Parameters
    ----------
    name : str
        Entity full name.
    currency : .entity.Currency
        Currency of asset pricing.
    quote_units : {'units', 'cents'}, optional
        Price quotations are either in currency units (default) or currency
        cents.
    owner : .entity.Entity, optional
        Share owner entity.

    Warning
    -------
    An instance of this class may be an index of a basket of underlying
    invest-able assets. Then it is not a data-like index of the ``Index``
    class but rather an instance of this the ``Asset`` class. Not all these
    indices are however invest-able.

    In the case of this class being an index, an index may be an imaginary
    portfolio of securities representing a particular market or a portion of
    it. Each index has its own calculation methodology and is usually expressed
    in terms of a change from a base value. Thus, the percentage change is more
    important than the actual numeric value.

    The Standard & Poor's 500 is one of the world's best known indexes, and is
    the most commonly used benchmark for the stock market. Other prominent
    indexes include the DJ Wilshire 5000 (total stock market), the MSCI EAFE
    (foreign stocks in Europe, Australasia, Far East) and the Lehman Brothers
    Aggregate Bond Index (total bond market).

    Because, technically, you can't actually invest in an index, index mutual
    funds and exchange- traded funds (based on indexes) allow investors to
    invest in securities representing broad market segments and/or the total
    market.

    However some indices are invest-able and these often take the form of
    Exchange Traded Funds which are invest-able indexes listed on an exchange.
    (See the child class ``ListedEquity`` and it's child classes.)

    NOTE: Speak about ownership of an Asset by an Entity and how this helps
    create a directed, acyclic graph of Entity/Asset holdings.

    Note
    ----
    This class should not be directly instantiated.

    See also
    --------
    .Entity

    """

    __tablename__ = "asset"
    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
    }

    _id = Column(Integer, ForeignKey("asset_base._id"), primary_key=True)
    """ Primary key."""

    # An Entity owns many Assets. Each Asset has one owner Entity.
    # NOTE: Currently owner is allowed to be NULL. Make owner compulsory.
    _owner_id = Column(Integer, ForeignKey("entity._id"), nullable=True)
    owner = relationship("Entity", backref="asset_list", foreign_keys=[_owner_id])

    # NOTE: This (or child) is were we would add asset fundamental data relationships
    # NOTE: This (or child) is were we would add asset book relationships

    # Major asset class. This is a generic class so the asset class is
    # indeterminate.
    _asset_class = None

    def __init__(self, name, currency, quote_units="units", **kwargs):
        """Instance initialization."""

        # Asset owner
        if "owner" in kwargs:
            self.owner = kwargs.pop("owner")

        super().__init__(name, currency, quote_units, **kwargs)

    @property
    def _eod_series(self):
        """list: EOD historical time-series collection ranked by date_stamp."""
        return [s for s in self._time_series_single_item if isinstance(s, EODBase)]

    @property
    def domicile(self):
        """Defined as the owner domicile."""
        # NOTE: In future the owner may be forced to be not None
        if self.owner is None:
            return None
        else:
            return self.owner.domicile

    def get_asset_class(self):
        """Return the major asset class in lower-case.

        Typical major asset classes are:

        'cash':
            Cash as in notes or bank balance.
        'money':
            Short term fixed-interest securities.
        'bond':
            Debt instruments where the owner of the debts is owed by the issuer.
        'property':
            Investments into real-estate. Your home or investment property, plus
            shares of funds that invest in commercial real estate.
        'equity':
            Also called stocks. Shares in publicly held companies or company
            legal vehicles.
        'commodity':
            Physical goods such as gold, copper, crude oil, natural gas, wheat,
            corn, and even electricity.

        Returns
        -------
        str
        A string of one of the above major asset classes in lower-case. May be
        ``None`` for abstract super-classes such as
        ``.Asset``
        """

        return self._asset_class

    def get_eod_series(self):
        """Return the EOD time series for the asset.

        Returns
        -------
        pandas.DataFrame
            An End-Of-Day (EOD) ``pandas.DataFrame`` with columns
            identical to the keys from the ``time_series.SimpleEOD.to_dict()``
            or ``time_series.ListedEOD.to_dict()`` or polymorph class method.

        Raises
        ------
        EODSeriesNoData
            If no time series exists.
        """
        # Use direct SQL query for better performance than ORM
        from sqlalchemy import text
        session = object_session(self)

        query = text("""
        SELECT
            tsb.date_stamp,
            eod.price,
            teod.open,
            teod.close,
            teod.high,
            teod.low,
            teod.adjusted_close,
            teod.volume
        FROM time_series_base tsb
        LEFT JOIN simple_eod eod ON tsb._id = eod._id
        LEFT JOIN trade_eod teod ON eod._id = teod._id
        WHERE tsb._asset_id = :asset_id
            AND tsb._discriminator IN ('simple_eod', 'trade_eod', 'listed_eod',
                                       'listed_equity_eod', 'index_eod', 'forex_eod')
        ORDER BY tsb.date_stamp ASC
        """)

        data_frame = pd.read_sql(query, session.bind, params={'asset_id': self._id})

        if len(data_frame) == 0:
            raise EODSeriesNoData(f"Expected EOD data for {self.identity_code}.")

        # Handle quote_units conversion
        price_columns = ['price', 'open', 'close', 'high', 'low', 'adjusted_close']
        if self.quote_units == "cents":
            for col in price_columns:
                if col in data_frame.columns and data_frame[col].notna().any():
                    data_frame[col] = data_frame[col] / 100.0

        # Keep only non-null columns (depends on which type of EOD record)
        data_frame = data_frame.dropna(axis=1, how='all')
        data_frame["date_stamp"] = pd.to_datetime(data_frame["date_stamp"])
        data_frame.set_index("date_stamp", inplace=True)
        data_frame.sort_index(inplace=True)  # Assure ascending
        data_frame.name = self

        return data_frame

    def get_last_eod(self):
        """Return the last EOD for the asset.

        Returns
        -------
        .time_series.EODBase or polymorph child class
            The last ``.time_series.EODBase`` (or child class) time series
            instance.

        Raises
        ------
        EODSeriesNoData
            If no time series exists.
        """
        if len(self._eod_series) == 0:
            raise EODSeriesNoData(f"Expected EOD data for {self.identity_code}.")
        else:
            return self._eod_series[-1]

    def get_last_eod_date(self):
        """Return the date of the last EOD for the asset.

        Returns
        -------
        pandas.Timestamp
            The date of the last EOD for the asset.

        Raises
        ------
        EODSeriesNoData
            If no time series exists.
        """
        last_eod = self.get_last_eod()
        return last_eod.date_stamp

    def get_time_series_processor(self, price_item='price'):
        """Return a TimeSeriesProcessor for this asset.

        Parameters
        ----------
        price_item : str, optional
            The single price item column to keep. This argument is standardized
            across all asset classes and their time-series. In this class the
            value is required to be 'price' as the EOD time-series for this
            class only has a 'price' item (See to_dict() method of the
            ``time_series.EODBase`` class and its polymorphs).

        Returns
        -------
        .time_series_processor.TimeSeriesProcessor
            A ``.time_series_processor.TimeSeriesProcessor`` instance for this
            asset which includes only the EOD time series and `identity`
            columns set to this ``Asset`` instance.
        """
        # Check price item is valid
        eod = self.get_eod_series()
        if price_item not in eod.columns:
            raise ValueError(
                f"Unexpected `price_item` argument {price_item}. "
                f"Expected one of {list(eod.columns)}.")

        prices_df = eod.reset_index()
        prices_df["identity"] = self
        columns_to_keep = ["identity", "date_stamp", price_item]
        prices_df = prices_df[columns_to_keep]

        tsp = TimeSeriesProcessor(prices_df=prices_df)
        return tsp

class Cash(Asset):
    """Cash in currency held.

    In English vernacular cash refers to money in the physical form of
    currency, such as banknotes and coins.

    In bookkeeping and finance, cash refers to current assets comprising
    currency or currency equivalents that can be accessed immediately or
    near-immediately (as in the case of money market accounts). Cash is seen
    either as a reserve for payments, in case of a structural or incidental
    negative cash flow or as a way to avoid a downturn on financial markets.

    Parameters
    ----------
    currency : .asset.Currency
        The currency of the cash asset

    Currency is named after the currency itself and its ticker is that of the
    currency. Currency although it is an ``Asset`` has no ownership as it is not
    owned by an entity. Quote units are always in 'units'.

    See also
    --------
    .Asset, .Currency, .Domicile

    Attributes
    ----------
    ticker : str
        ISO 4217 3-letter currency code used as the ticker.
    """

    __tablename__ = "cash"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    _id = Column(Integer, ForeignKey("asset._id"), primary_key=True)

    KEY_CODE_LABEL = "asset_currency"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    _asset_class = "cash"

    def __init__(self, currency):
        """Instance initialization."""
        # The name is constrained to that of the currency.
        name = currency.name

        # Quote units always in 'units' for cash
        super().__init__(name, currency, quote_units="units")

    def __repr__(self):
        """Return the official string output."""
        msg = "{}(currency={!r})".format(self.__class__.__name__, self.currency)

        return msg

    def _get_identity_code(self):
        """Required for unique identification of instances and is not optional."""
        return self.ticker

    @property
    def ticker(self):
        """ISO 4217 3-letter currency code."""
        return self.currency.ticker

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.ticker

    @property
    def long_name(self):
        """str: Return the long name string."""
        msg = "{} is an {} priced in {}.".format(
            self.name, self.__class__.__name__, self.currency.ticker
        )

        return msg

    def get_locality(self, domicile_code):
        """Return the locality "domestic" or "foreign".

        The "domestic" or "foreign" status of an asset in a current account is
        determined as:

        Parameters
        ----------
        domicile_code : str(2)
            ISO 3166-1 Alpha-2 two letter country code of the current account's
            domicile. The asset's domestic or foreign status shall be determined
            relative to this.

        Return
        ------
        str
            The asset's domestic or foreign status relative to the current
            account domicile. The possible values are:

            'domestic':
                If the domicile code of the current account and the asset agree.
                An example would be a South African custody account holding an
                domestic asset of South African domicile.
            'foreign':
                If the domicile code of the current account and the asset
                disagree. An example would be a South African custody account
                holding an foreign asset of German domicile.

        """
        if self.currency.in_domicile(domicile_code):
            locality = "domestic"
        else:
            locality = "foreign"

        return locality

    @classmethod
    def factory(cls, session, ticker, create=True, **kwargs):
        """Manufacture/retrieve an instance from the given parameters.

        If a record of the specified class instance does not exist then add it,
        else do nothing. Then return the instance.

        Factory Method Behaviour
        ------------------------
        This factory operates in two modes controlled by the ``create`` parameter:

        **Retrieval Mode** (create=False):
            Retrieves an existing Cash instance by ticker. Raises ``FactoryError``
            if not found.

            Example::

                # Must already exist in database
                cash = Cash.factory(session, ticker="USD", create=False)

        **Creation Mode** (create=True, default):
            Retrieves existing Cash or creates new one if missing. **Important**:
            The specified currency must already exist or ``FactoryError`` is raised.

            Example::

                # Currency "USD" must already exist
                cash = Cash.factory(session, ticker="USD")

        **Dependency Enforcement**:
            This factory calls ``Currency.factory(session, ticker)`` in retrieval
            mode (without name or country_code_list), ensuring the currency must
            pre-exist. This prevents accidental creation of Currency records.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        ticker : str(3)
            ISO 4217 3-letter currency code.
        create : bool, optional
            If False, raises ``FactoryError`` if cash asset doesn't exist. If True
            (default), creates cash asset if missing. Default is True.
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        Cash
            The single instance that is in the session.

        Raises
        ------
        FactoryError
            If cash not found when create=False, or if specified currency
            doesn't exist.

        See Also
        --------
        Currency.factory : Called in retrieval mode to get currency

        """
        # Check if entity exists in the session and if not then add it.
        try:
            obj = session.query(cls).filter(cls.ticker == ticker).one()
        except NoResultFound:
            # Raise exception if the currency is not found
            if not create:
                raise FactoryError(f"Currency with ticker `{ticker}` not found.")
            else:
                # Create a new instance, fetch pre-existing currency
                currency = Currency.factory(session, ticker)
                obj = cls(currency, **kwargs)
                session.add(obj)
        else:
            # There would never be changes to Cash to reconcile so just pass
            pass

        return obj

    @classmethod
    def update_all(cls, session):
        """Update/create Cash instances for all currencies in the session.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.

        Note
        ----
        No time-series data is updated by this method as cash has no time-series data and
        always has a price of 1.0 currency unit.
        """

        # A cash instance for every currency
        currency_list = session.query(Currency).all()
        if len(currency_list) == 0:
            raise Exception("No Currency instances found. ")
        for currency in currency_list:
            cls.factory(session, currency.ticker)

    def get_eod_series(self, date_index):
        """Return the EOD time series for the Cash object.

        The price of a cash unit is always 1.0 currency unit.

        Parameters
        ----------
        date_index : pandas.DatetimeIndex
            The time-series dates of other entities that the ``Cash``
            time-series are to be appended to. Without this parameter it is
            impossible to know in advance how long the cash time-series is
            required to be.

        Returns
        -------
        pandas.DataFrame
            An End-Of-Day (EOD) ``pandas.DataFrame`` with columns identical to
            the keys from the ``time_series.SimpleEOD.to_dict()`` or
            ``time_series.ListedEOD.to_dict()`` or polymorph class method.

        Raises
        ------
        ValueError
            If the `date_index` argument is not a ``pandas.DatetimeIndex`` or
            is empty.

        """
        if not isinstance(date_index, pd.DatetimeIndex):
            raise ValueError("Unexpected date_index argument type.")
        if len(date_index) == 0:
            raise ValueError("Empty date_index argument.")

        # Make a list of dicts for a price of 1 currency unit per date
        if self.quote_units == "cents":
            price = 100.0
        else:
            price = 1.0
        trade_eod_dict_list = [{"date_stamp": date, "price": price} for date in date_index]

        data_frame = pd.DataFrame(trade_eod_dict_list)
        data_frame["date_stamp"] = pd.to_datetime(data_frame["date_stamp"])
        data_frame.set_index("date_stamp", inplace=True)
        data_frame.sort_index(inplace=True)  # Assure ascending
        data_frame.name = self

        return data_frame

    def get_time_series_processor(self, date_index, price_item='price'):
        """Return a TimeSeriesProcessor for this asset.

        Parameters
        ----------
        date_index : pandas.DatetimeIndex
            The time-series dates of other entities that the ``Cash``
            time-series are to be appended to. Without this parameter it is
            impossible to know in advance how long the cash time-series is
            required to be.
        price_item : str, optional
            The single price item column to keep. This argument is standardized
            across all asset classes and their time-series. In this class the
            value is required to be 'price' as the EOD time-series for this
            class only has a 'price' item (See to_dict() method of the
            ``time_series.EODBase`` class and its polymorphs).

        Returns
        -------
        .time_series_processor.TimeSeriesProcessor
            A ``.time_series_processor.TimeSeriesProcessor`` instance for this
            asset which includes only the Cash EOD time series of price = 1.0
            with dates from the `date_index` argument and `identity`
            columns set to this ``Cash`` instance.
        """
        # Check that the price_item argument is 'price for this Cash class
        if price_item != 'price':
            raise ValueError(
                f"Unexpected `price_item` argument `{price_item}`. "
                "Expected 'price' for this asset class.")

        prices_df = self.get_eod_series(date_index).reset_index()
        prices_df["identity"] = self
        columns_to_keep = ["identity", "date_stamp", price_item]
        prices_df = prices_df[columns_to_keep]

        tsp = TimeSeriesProcessor(prices_df=prices_df)

        return tsp


class Forex(Cash):
    """Currency exchange rates.

    A currency - the `base_currency` has it's price expressed in the
    `price_currency`. For example: The United Stated Dollar (USD) has its
    price in Japanese Yen (JPY) and in 2022/06/22 the price of 1 USD was 135
    JPY. USD may be considered the primary currency. JPY may be considered the
    secondary currency. As such the code for this exchange rate shall be USDJPY
    and may be read as USD to JPY, i.e., 1 USD to 135 JPY, or 1USD costs 135JPY.

    All stored forex rates EOD time-series will have as their ``base_currency``
    be the ``root_currency_ticker``. Arbitrary rates will then be calculated off
    these stored rates.

    Note
    ----
    The notion of a currency conversion of a price in a base currency may be
    thought of as a multiplication of a that price by a factor with the
    price currency being the numerator and the base currency the
    denominator. So:

        ```
            price    = price    * EURUSD
                       USD        EUR
        ```

    Warning
    -------
    For this version the ``base_currency`` shall be asserted to be the same as
    the toot currency (USD) or an assertion exception shall be raised. This
    therefore also applies then too to the ``factory`` and ``update_all``
    methods. In future versions, to avoid this issue, the ``update_all`` method
    will automate the collation of diverse rates into a rate suite with one
    ``base_currency``, i.e., the ``root_currency_ticker``.

    Parameters
    ----------
    base_currency : .asset.Currency
        The currency that is priced is the `price_currency`. In the example
        above this is USD. It may be considered the primary currency. It may
        also be considered the asset-to-be-priced.
    price_currency : .asset.Currency
        The pricing currency. In the example above this is JPY. It may be
        considered the secondary currency. It may also be considered the foreign
        currency price of the asset-to-be-priced.


    """

    __tablename__ = "forex"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    _id = Column(Integer, ForeignKey("cash._id"), primary_key=True)

    KEY_CODE_LABEL = "ticker"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    _asset_class = "forex"

    # Priced currency, or ``base_currency``
    _currency_id2 = Column(Integer, ForeignKey("currency._id"), nullable=False)
    base_currency = relationship(Currency, foreign_keys=[_currency_id2])

    # Currency ticker is redundant information, but very useful and inexpensive
    ticker = Column(String(6))

    # There can be only one forex instance for a given
    # base_currency/price_currency which is already encoded in the
    # ticker column at class initialization.
    __table_args__ = (UniqueConstraint("ticker"),)

    # The financial_data EOD_GET_METHOD method overridden here
    EOD_GET_METHOD = AssetBase.HISTORY_INSTANCE.get_forex_eod

    # Associated time-series class for this asset class. This must be overridden
    # in child classes.
    TIME_SERIES_CLASS = ForexEOD

    # The reference or root ticker. Its price will always be 1.0.
    root_currency_ticker = "USD"

    # List of top foreign currencies. Their time series are maintained as the
    # price of 1 unit of the ``root_currency_ticker``. South African ZAR is included for
    # domestic reasons.
    foreign_currencies_list = [
        "USD",
        "EUR",
        "GBP",
        "CAD",
        "AUD",
        "JPY",
        "CHF",
        "CNY",
        "HKD",
        "NZD",
        "SEK",
        "KRW",
        "SGD",
        "NOK",
        "MXN",
        "INR",
        "RUB",
        "ZAR",
    ]

    def __init__(self, base_currency, price_currency):
        """Instance initialization."""
        # FIXME: For this version assert the ``base_currency`` to be the ``root_currency_ticker`` and state clearly in the documentation

        # Expect the `base_currency` to be the root currency (USD).
        if base_currency.ticker != self.root_currency_ticker:
            raise AssertionError(
                "Expected the `base_currency` to be the root currency (USD)."
            )
        self.base_currency = base_currency

        # The pricing currency the denominator and the base currency is the
        # numerator of the exchange rate. We expect the base currency to have
        # been already set so that the superclass can create the identity_code
        # attribute which is also used as the forex's ticker.
        super().__init__(price_currency)

        # The ticker and name are the two joined ISO 4217 3-letter currency
        # codes of the base and price currencies. For example, USDJPY for the
        # price of 1 USD in JPY.
        self.ticker = self.identity_code
        self.name = self.identity_code  # Override the name to be the ticker

    def __repr__(self):
        """Return the official string output."""
        return "{}(base_currency={!r}, price_currency={!r})".format(
            self.__class__.__name__, self.base_currency.ticker, self.currency.ticker
        )

    def _get_identity_code(self):
        """Required for unique identification of instances and is not optional."""
        return self._get_forex_ticker()

    def _get_forex_ticker(self):
        """Return the forex double currency ticker code."""
        return f"{self.base_currency.ticker}{self.currency.ticker}"

    @property
    def base_currency_ticker(self):
        """ISO 4217 3-letter ``base_currency`` code."""
        return self.base_currency.ticker

    @property
    def price_currency_ticker(self):
        """ISO 4217 3-letter price currency (foreign currency) code."""
        return self.currency.ticker

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return "{}{}".format(self.base_currency.ticker, self.currency.ticker)

    @property
    def long_name(self):
        """str: Return the long name string."""
        return "One {} priced in {}".format(
            self.base_currency.ticker, self.currency.ticker
        )

    @classmethod
    def factory(cls, session, base_ticker, price_ticker, create=True, **kwargs):
        """Manufacture/retrieve an instance from the given parameters.

        If a record of the specified class instance does not exist then add it,
        else do nothing. Then return the instance.

        Factory Method Behaviour
        ------------------------
        This factory operates in two modes controlled by the ``create`` parameter:

        **Retrieval Mode** (create=False):
            Retrieves an existing Forex pair by base and price tickers. Raises
            ``FactoryError`` if not found.

            Example::

                # Must already exist in database
                forex = Forex.factory(
                    session, base_ticker="USD", price_ticker="EUR",
                    create=False
                )

        **Creation Mode** (create=True, default):
            Retrieves existing Forex or creates new one if missing. **Important**:
            Both currencies must already exist or ``FactoryError`` is raised.

            Example::

                # Both "USD" and "EUR" currencies must already exist
                forex = Forex.factory(
                    session, base_ticker="USD", price_ticker="EUR"
                )

        **Dependency Enforcement**:
            This factory calls ``Currency.factory(session, ticker)`` in retrieval
            mode for both base and price currencies, ensuring both must pre-exist.
            This prevents accidental creation of Currency records.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        base_ticker : str(3)
            ISO 4217 3-letter currency code. The priced or ``base_currency``.
        price_ticker : str(3)
            ISO 4217 3-letter currency code. The price currency.
        create : bool, optional
            If False, raises ``FactoryError`` if forex pair doesn't exist. If True
            (default), creates forex pair if missing. Default is True.
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        Forex
            The single instance that is in the session.

        Raises
        ------
        FactoryError
            If forex not found when create=False, or if either specified currency
            doesn't exist.

        See Also
        --------
        Currency.factory : Called in retrieval mode for both currencies

        """
        # Get the ``base_currency`` if it exits
        try:
            base_currency = Currency.factory(session, base_ticker)
        except NoResultFound:
            raise FactoryError("Base currency %s not found", base_ticker)
        # Get the pricing currency if it exits
        try:
            price_currency = Currency.factory(session, price_ticker)
        except NoResultFound:
            raise FactoryError("Base currency %s not found", price_ticker)

        # Check if entity exists in the session and if not then add it.
        try:
            obj = (
                session.query(cls)
                .filter(
                    cls.base_currency == base_currency,
                    cls.currency == price_currency,
                )
                .one()
            )
        except NoResultFound:
            # Raise exception if the currency is not found
            if not create:
                ticker = "{}{}".format(base_ticker, price_ticker)
                raise FactoryError(f"Forex {ticker} not found.")
            else:
                # Create a new instance, fetch pre-existing currency
                obj = cls(base_currency, price_currency, **kwargs)
                session.add(obj)
        else:
            # There would never be changes to Cash to reconcile so just pass
            pass

        return obj

    @classmethod
    def update_meta_data(cls, session):
        """Update/create instances of the Forex class.

        The Forex instances are created based on the list of foreign currencies
        defined in the class attribute ``foreign_currencies_list`` and the root
        currency defined in the class attribute ``root_currency_ticker``. The
        method creates Forex instances for all combinations of the root currency
        and the foreign currencies in the list.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.

        """
        # Create Forex instances as per the Forex.foreign_currencies list
        # attribute
        foreign_currencies_list = (
            session.query(Currency)
            .filter(Currency.ticker.in_(cls.foreign_currencies_list))
            .all()
        )
        if len(foreign_currencies_list) == 0:
            raise Exception("No Currency instances found.")
        if len(cls.foreign_currencies_list) != len(foreign_currencies_list):
            raise FactoryError("Not all foreign currencies were found.")

        # Bulk add/update metadata uses the class factory method
        for price_currency in foreign_currencies_list:
            cls.factory(session, cls.root_currency_ticker, price_currency.ticker)

    @classmethod
    def update_all(cls, session):
        """Update/create Forex instances and their trade time-series data.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        """
        # Bulk add/update metadata uses the class factory method an creates
        # Forex based on Currency instances.
        cls.update_meta_data(session)

        # Get the list of foreign currencies in the database
        foreign_currencies_list = session.query(Forex).all()

        # Update EOD trade time-series data for Forex.
        cls.update_eod_time_series(session, foreign_currencies_list)

    @classmethod
    def get_rates_data_frame(
        cls, session, base_ticker, price_ticker_list, price_item="close"
    ):
        """Price the base in a list of pricing currencies.

        Note
        ----
        The notion of a currency conversion of a price in a base currency may be
        thought of as a multiplication of a that price by a factor with the
        price currency being the numerator and the base currency the
        denominator. So:

            ```
                price    = price    * EURUSD
                     USD        EUR
            ```

        Parameter
        ---------
        base_ticker : str(3)
            ISO 4217 3-letter currency code. The desired new ``base_currency``,
            i.e., the currency to be priced in the other currencies defined by
            the ``price_ticker_list``.
        price_ticker_list : list
            ISO 4217 3-letter currency code. The list of pricing currency
            tickers with which to price the base currency.
        price_item : str
            The specific item of price such as 'close', 'open', `high`, or
            `low`. Only valid when the ``series`` argument is set to 'price'.

        Returns
        -------
        pandas.DataFrame
            There shall be one rate column per ``price_ticker_list`` item, each
            column being the desired price of the base currency in the price
            currency.
        """
        # Get the `root_currency_ticker` prices in the currencies defined in the
        # price_ticker_list argument
        eod_dict = dict()
        for price_ticker in price_ticker_list:
            forex = cls.factory(session, cls.root_currency_ticker, price_ticker)
            eod = forex.get_eod_series()
            eod_dict[price_ticker] = eod[price_item]
        df_eod_prices = pd.DataFrame(eod_dict)
        # Keep last price over holiday periods
        df_eod_prices.ffill(axis="index", inplace=True)

        # Get the `root_currency_ticker` prices in the desired `base_ticker`
        # currency.
        forex = cls.factory(session, cls.root_currency_ticker, base_ticker)
        eod = forex.get_eod_series()
        series_eod_base = eod[price_item]
        # Condition the index to that of df_eod_prices in preparation to become
        # the denominator, then forward fill last price over holiday or break
        # periods.
        series_eod_base = series_eod_base.reindex(
            index=df_eod_prices.index, method="ffill"
        )

        # Price the base currency in the pricing currencies.
        df_rates = df_eod_prices.divide(series_eod_base, axis="index")

        return df_rates


class Share(Asset):
    """A financial asset that is a share that has been issued.

    Note
    ----
    This is an abstract class not meant for direct instantiation.

    The domicile (and therefore also the currency) of a share is that of the
    issuer. This may not always be the case and a software upgrade in the future
    may address this.

    Parameters
    ----------
    name : str
        Entity full name.
    issuer: .Issuer
        The issuing institution that issues the asset for exchange.
    currency : .entity.Currency, optional
        Currency of asset pricing. If omitted then the currency is that of the
        issuer domicile which is the usual case.
    quote_units : {'units', 'cents'}, optional
        Price quotations are either in currency units (default) or currency
        cents.
    shares_in_issue : int, optional
        Number of shares in issue.
    distributions : bool, optional
        Does the share pay distributions or not. Default is `False`.
    owner : .entity.Entity, optional
        Share owner entity.

    Attributes
    ----------
    issuer: .Issuer
        The issuing institution that issues the asset for exchange.

    See also
    --------
    .Asset, .Issuer

    """

    # NOTE: Create Account class to  (hold) assets with many-to-one relationship

    __tablename__ = "share"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    _id = Column(Integer, ForeignKey("asset._id"), primary_key=True)
    """ Primary key."""

    # NOTE: Here we would add share unitization and ownership relationships

    # Issuer issues Shares. Each Share has one Issuer.
    _issuer_id = Column(Integer, ForeignKey("issuer._id"), nullable=False)
    issuer = relationship("Issuer", backref="share_list")

    # Number of share units issued byu the Issuer
    shares_in_issue = Column(Integer, nullable=True)

    # True of a share pays distributions such as dividends or interest, else
    # False. Default is False.
    distributions = Column(Boolean, nullable=False, default=False)

    def __init__(self, name, issuer, currency, **kwargs):
        """Instance initialization."""
        # If the currency is not provided then the currency is the issuer's
        # domicile's currency
        if currency is None:
            currency = issuer.domicile.currency


        self.issuer = issuer

        # Number of shares issued by the Issuer
        if "shares_in_issue" in kwargs:
            self.shares_in_issue = kwargs.pop("shares_in_issue")

        # Does the share pay distributions or not
        if "distributions" in kwargs:
            self.distributions = kwargs.pop("distributions")
        else:
            # Not sure why the default isn't being set to False as specified in
            # the column attribute definition, so we do it here anyway
            self.distributions = False

        super().__init__(name, currency, **kwargs)

    @property
    def domicile(self):
        """.entity.Domicile : Same as that of ``Share`` ``Issuer``."""
        return self.issuer.domicile

    def get_locality(self, domicile_code):
        """Return the locality "domestic" or "foreign".

        The "domestic" or "foreign" status of an asset in a current account is
        determined as:

        Parameters
        ----------
        domicile_code : str(2)
            ISO 3166-1 Alpha-2 two letter country code of the current account's
            domicile. The asset's domestic or foreign status shall be determined
            relative to this.

        Return
        ------
        str
            The asset's domestic or foreign status relative to the current
            account domicile. The possible values are:

            'domestic':
                If the domicile code of the current account and the asset agree.
                An example would be a South African custody account holding an
                domestic asset of South African domicile.
            'foreign':
                If the domicile code of the current account and the asset
                disagree. An example would be a South African custody account
                holding an foreign asset of German domicile.

        """
        share_domicile_code = self.domicile.country_code
        if domicile_code == share_domicile_code:
            locality = "domestic"
        else:
            locality = "foreign"

        return locality


class Listed(Share):
    """Any kind of listed financial share.

    An International Securities Identification Number (ISIN) uniquely identifies
    a security. An ISIN consists of three parts: Generally, a two letter
    country code, a nine character alpha-numeric national security identifier,
    and a single check digit. The country code is the ISO 3166-1 alpha-2 code
    for the country of issue, which is not necessarily the country where the
    issuing company is domiciled. International securities cleared through
    Clearstream or Euroclear, which are worldwide, use "XS" as the country code.

    Securities to which ISINs can be issued include debt securities, shares,
    options, derivatives and futures. The ISIN identifies the security, not the
    exchange (if any) on which it trades; it is not a ticker symbol. For
    instance, stock trades through almost 30 trading platforms and exchanges
    worldwide, and is priced in five different currencies; it has the same ISIN
    on each, though not the same ticker symbol. ISIN cannot specify a
    particular trading location in this case, and another identifier, typically
    MIC (Market Identification Code) or the three-letter exchange code, will
    have to be specified in addition to the ISIN. The Currency of the trade
    will also be required to uniquely identify the instrument using this
    method.

    Parameters
    ----------
    name : str
        Entity full name.
    issuer: .Issuer
        The issuing institution that issues the asset for exchange.
    isin : str
        An International Securities Identification Number (ISIN) uniquely
        identifies a security. ISINs consist of two alphabetic characters, which
        are the ISO 3166-1 alpha-2 code for the issuing country (An ISIN cannot
        specify a particular trading location.), nine alpha-numeric characters
        (the National Securities Identifying Number, or ISIN, which identifies
        the security, padded as necessary with leading zeros), and one numerical
        check digit.
    exchange : .Exchange
        The exchange the asset is listed upon.
    ticker : str
        The ticker assigned to the asset by the exchange listing process.
    status : str
        Flag of listing status ('listed', 'delisted').
    quote_units : {'units', 'cents'}, optional
        Price quotations are either in currency units (default) or currency
        cents.
    shares_in_issue : int, optional
        Number of shares in issue.
    distributions : bool, optional
        Does the share pay distributions or not. Default is `False`.
    owner : .entity.Entity, optional
        Share owner entity.


    Note
    ----
    The domicile is constrained to the issuer domicile.

    Note
    ----
    The currency is that of the exchange domicile.

    Raises
    ------
    The domicile of the Listed share shall be constrained to the country
    represented by the first two letters, the ISO 3166-1 alpha-2 country code.
    A ``ValueError`` exception shall be raised if this fails.

    See also
    --------
    .Share, .Issuer, .Exchange

    """

    __tablename__ = "listed"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    _id = Column(Integer, ForeignKey("share._id"), primary_key=True)
    """ Primary key."""

    # Exchange lists Listed. Each Listed has one Exchange.
    _exchange_id = Column(Integer, ForeignKey("exchange._id"), nullable=False)
    exchange = relationship("Exchange", backref="securities_list")

    # Ticker on the listing exchange (Uses exchange MIC). MIC is the ISO 10383
    # Market Identifier Code which is a unique identification code used to
    # identify securities trading exchanges. The MIC could be accessed through
    # the exchange relationship, but it is also stored here for query
    # convenience and to enforce the unique constraint on the combination of MIC
    # and ticker.
    mic = Column(String(4), nullable=False)
    ticker = Column(String(12), nullable=False)

    # The National Securities Identifying Number (ISIN) is a unique identifier
    # for the security. It is not a ticker symbol and does not specify a
    # particular trading location. The ISIN consists of two alphabetic
    # characters (the ISO 3166-1 alpha-2 code for the issuing country), nine
    # alpha-numeric characters (the National Securities Identifying Number,
    # padded as necessary with leading zeros), and one numerical check digit.
    # The ISIN is unique across all exchanges, while the combination of exchange
    # MIC and ticker it is the ticker that is unique for that exchange.
    isin = Column(String(12), nullable=False)

    # Each ISIN is unique and each Exchange/ticker pair is unique
    __table_args__ = (
        UniqueConstraint("mic", "ticker"),
        UniqueConstraint("isin"),
    )

    KEY_CODE_LABEL = "isin"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    # Listing status.
    status = Column(Enum("listed", "delisted"), nullable=False)

    # Associated time-series class override for this asset class.
    TIME_SERIES_CLASS = ListedEOD

    @staticmethod
    def _check_isin(isin):
        """Check to see if the isin number provided is valid."""
        if stdisin.is_valid(isin):
            # Convert the number to the minimal representation. This strips
            # the number of any valid separators and removes surrounding
            # whitespace.
            isin = stdisin.compact(isin)
        else:
            raise BadISIN(isin)

        return isin

    def __init__(self, name, issuer, isin, exchange, ticker, status, **kwargs):
        """Instance initialization."""
        # Check to see if the isin number provided is valid. This checks the
        # length and check digit.
        isin = Listed._check_isin(isin)
        # Check issuer domicile against the 1st two ISIN letters (ISO 3166-1
        # alpha-2 code)
        if isin[0:2] == issuer.domicile.country_code:
            self.isin = isin
        else:
            raise ValueError("Unexpected domicile. Does not match ISIN country code.")

        # Do no remove this code!!. Some methods that use this class (such as
        # factory methods) are able to place arguments with a None value, this
        # circumventing Python's positional-arguments checks. Check manually
        # them here.
        if all([name, issuer, isin, exchange, ticker, status]):
            pass
        else:
            raise ValueError("Unexpected `None` value for some positional arguments.")

        # De-listed  often carry the same name as the listed share, so when the
        # status is de-listed we append the status to the name for uniqueness
        # and clarity. For example, if the share name is "ABC Ltd" and the
        # status is "delisted" then the name of the instance will be "ABC Ltd
        # (delisted)". This is because there may be a need to have both the
        # listed and de-listed share in the database at the same time, and if
        # they have the same name then it is not clear which is which.
        if status == "delisted":
            name = f"{name} ({status})"

        # Some shares carry the same name as the issuer, if so we append the
        # widely used eodhistoricaldata.com, exchange code to the name for
        # uniqueness and clarity. For example, if the share name is "ABC Ltd"
        # and the exchange is "Johannesburg Stock Exchange" then the name of the
        # instance will be "ABC Ltd (Johannesburg Stock Exchange)". This is
        # because there may be a need to have the same share listed on multiple
        # exchanges in the database at the same time.
        if name == issuer.name:
            name = f"{name} ({exchange.eod_code})"

        # The currency is that of the exchange domicile. This is because the
        # price of a share is that of the exchange listing, and the exchange
        # listing is in the exchange domicile currency. This is the usual case,
        # but there may be exceptions. This will be addressed in a future
        # software upgrade if it becomes an issue.
        currency = exchange.domicile.currency

        self.exchange = exchange
        self.mic = exchange.mic
        self.ticker = ticker
        self.status = status

        super().__init__(name, issuer, currency, **kwargs)

    def __repr__(self):
        """Return the official string output."""
        return '{}(name="{}", issuer={!r}, isin="{}", exchange={!r}, ticker="{}", status="{}")'.format(
            self.__class__.__name__, self.name, self.issuer, self.isin, self.exchange, self.ticker, self.status
        )

    def _get_identity_code(self):
        """Required for unique identification of instances and is not optional."""
        # The identity code is the combination of the ticker and the widely used
        # eodhistoricaldata.com exchange code. This is because the ticker is not
        # unique across all exchanges, but the combination of ticker and
        # exchange code is unique. For example, if there are two shares with the
        # same ticker "ABC" listed on two different exchanges with codes "X" and
        # "Y", then their identity codes will be "ABC.X" and "ABC.Y"
        # respectively, which are unique.
        return self.ticker + "." + self.exchange.eod_code

    @property
    def domicile(self):
        """.entity.Domicile : ``Domicile`` of the ``Listed``'s ``Exchange``."""
        return self.exchange.domicile

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.isin

    @property
    def long_name(self):
        """str: Return the long name string."""
        return ("{} ({}.{}) ISIN:{} is a {} on the {} issued by {} in {}").format(
            self.name,
            self.ticker,
            self.exchange.mic,
            self.isin,
            self._discriminator,
            self.exchange.name,
            self.issuer.name,
            self.domicile.country_name,
        )

    def get_locality(self, domicile_code):
        """Return the locality "domestic" or "foreign".

        The "domestic" or "foreign" status of listing held in a current account
        is determined as:

        'domestic':
            If the domicile code of the current account and the listing's
            exchange domicile code agree. An example would be a South African
            custody account holding a domestic asset listed on a South African
            exchange.
        'foreign':
            If the domicile code of the current account and the listing's
            exchange domicile code disagree. An example would be a South African
            custody account holding a foreign asset listed on a German exchange.

        Parameters
        ----------
        domicile_code : str(2)
            ISO 3166-1 Alpha-2 two letter country code of the current account's
            domicile. The asset's domestic or foreign status shall be determined
            relative to this.

        Return
        ------
        str
            The asset's domestic or foreign status relative to the current
            account domicile. See above examples.

        """
        listed_domicile_code = self.exchange.domicile.country_code
        if listed_domicile_code == domicile_code:
            locality = "domestic"
        else:
            locality = "foreign"

        return locality

    def to_dict(self):
        """Convert class data attributes into a factory compatible dictionary.

        Returns
        -------
        dict
            The dictionary keys are the same as the class' ``factory`` method
            argument names, with the exception of the ``cls``, ``session`` and
            ``create`` arguments.

        """
        return {
            "isin": self.isin,
            "mic": self.exchange.mic,
            "ticker": self.ticker,
            "listed_name": self.name,
            "issuer_name": self.issuer.name,
            "issuer_domicile_code": self.issuer.domicile.country_code,
            "status": self.status,
        }

    @classmethod
    def factory(
        cls,
        session,
        isin=None,
        mic=None,
        ticker=None,
        listed_name=None,
        issuer_domicile_code=None,
        issuer_name=None,
        status=None,
        create=True,
        **kwargs,
        ):
        """Retrieve or create a ``Listed`` instance.

        The factory always *tries to retrieve first*, using either the
        ``isin`` *or* the (``mic``, ``ticker``) pair as the lookup key. If a
        matching row is found it is returned (and optionally reconciled with
        any non-``None`` parameters). If none is found and ``create`` is
        ``True``, a new ``Listed`` is created; otherwise a ``FactoryError`` is
        raised.

        Modes
        -----
        **Retrieval** (no new row is created):
        - At least one of the following identifier sets must be supplied:
            - ``isin``
            - both ``mic`` and ``ticker``
        - ``create`` may be ``True`` or ``False``; retrieval is always
            attempted first.
        - If no existing instance is found and ``create`` is ``False``, a
            ``FactoryError`` is raised.

        **Creation** (row is created only when missing and ``create`` is True):
        - If retrieval fails and ``create`` is ``True``, the following
            arguments are required in addition to a valid identifier
            (``isin`` and ``ticker``):
            - ``listed_name``
            - ``issuer_name``
            - ``issuer_domicile_code``
            - ``mic``
        - The referenced ``Exchange`` and ``Issuer`` (and their domiciles) are
            resolved via their respective ``factory`` methods and must already
            exist; otherwise a ``FactoryError`` is raised.

        Reconciliation of existing rows
        --------------------------------
        When an existing instance is found, selected fields are updated from
        the arguments *if* non-``None`` values are provided and consistent:
        - ``listed_name``, ``mic``, ``ticker`` and ``status`` may be updated.
        - The issuer is treated as immutable: providing an ``issuer_name`` or
            ``issuer_domicile_code`` that conflicts with the stored issuer
            raises ``ReconcileError``.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        isin : str, optional
            An International Securities Identification Number (ISIN) uniquely
            identifies a security. Required for creation, or can be used alone
            for retrieval.
        mic : str, optional
            ISO 10383 MIC (Market Identifier Code) of the exchange. Required for
            creation, or can be used with ticker for retrieval.
        ticker : str, optional
            The ticker assigned to the asset by the exchange listing process.
            Required for creation, or can be used with mic for retrieval.
        listed_name : str, optional
            Entity full name of the listed security as it was issued. Required
            for creation.
        issuer_domicile_code : str(2), optional
            ISO 3166-1 Alpha-2 two letter country code. The domicile code of the
            issuer. Required for creation. The domicile must already exist.
        issuer_name : str, optional
            The name of the issuer institution that issued the share. Required
            for creation.
        status : str, optional
            Flag of listing status ('listed', 'delisted').
        create : bool, optional
            If False, raises ``FactoryError`` if listed security doesn't exist.
            If True (default), creates security if missing. Default is True.
        **kwargs
            Additional keyword arguments (e.g., quote_units, shares_in_issue).

        Returns
        -------
        Listed
            The single instance that is in the session.

        Raises
        ------
        FactoryError
            If listed not found when create=False, if required parameters missing
            for creation, or if issuer/exchange dependencies don't exist.
        ValueError
            If neither isin nor (mic, ticker) pair provided for retrieval.

        See Also
        --------
        Issuer.factory : Called to get or validate issuer
        Exchange.factory : Called to get or validate exchange

        Notes
        -----
        An instance may be retrieved by either:
            * The ``isin`` parameter alone.
            * The ``ticker`` and ``mic`` pair.

        The exchange domicile is considered to be the domicile of the listed
        share. If the parameters don't reflect that an exception shall be raised.

        To add a new listing to the session all the parameters except ``mic``
        are required. However if ``mic`` is specified then ``exchange_name``
        and ``exchange_domicile_code`` are not required.

        """
        if isin is not None:
            isin = Listed._check_isin(isin)  # Check ISIN for integrity.

        # Try to retrieve the instance by either ISIN or (MIC, ticker) pair.
        try:
            # Choose query method based on arguments
            if isin is not None:
                obj = session.query(cls).filter(cls.isin == isin).one()
            elif mic is not None and ticker is not None:
                obj = (
                    session.query(cls)
                    .filter(
                        # Must use explicit join in this line!
                        cls._exchange_id == Exchange._id
                    )
                    .filter(Exchange.mic == mic, cls.ticker == ticker)
                    .one()
                )
            else:
                raise FactoryError(
                    "Expected arguments, single `isin` or `ticker`-`mic` pair.",
                    action="Retrieve Failed",
                )
        except NoResultFound:
            # Create and add a new instance below if allowed
            if not create:
                raise FactoryError("Listed ISIN={}, not found.".format(isin))
            # Need sufficient arguments. Due to argument default these can be
            # None
            if not all([listed_name, isin, ticker]):
                raise FactoryError(
                    "Expected  arguments `listed_name`, `isin`, `ticker`. "
                    "Some are None.",
                    action="Create Failed",
                )
            if not all([issuer_name, issuer_domicile_code]):
                raise FactoryError(
                    "Expected valid `issuer_name`, `issuer_domicile_code` "
                    "arguments. Some are None.",
                    action="Create failed",
                )
            if mic is None:
                raise FactoryError("Expect valid exchange MIC argument. Got None.")
            # Begin Listed creation process
            try:
                exchange = Exchange.factory(session, mic=mic)
            except FactoryError:
                # The exchange must already exist.
                raise FactoryError(f"Exchange {mic} not found.", action="Create Failed")
            try:
                issuer = Issuer.factory(session, issuer_name, issuer_domicile_code)
            except FactoryError:
                raise FactoryError(
                    "Could not create or retrieve the Issuer. "
                    "Check Issuer arguments.",
                    action="Create Failed",
                )
            # Now we have all required arguments to create
            obj = cls(listed_name, issuer, isin, exchange, ticker, status, **kwargs)
            session.add(obj)
        else:
            # Reconcile any changes between the retrieved object and the new
            # parameters
            if listed_name and listed_name != obj.name:
                obj.name = listed_name
            if mic and mic != obj.exchange.mic:
                obj.exchange = Exchange.factory(session, mic=mic)
            if ticker and ticker != obj.ticker:
                obj.ticker = ticker
            if status and status != obj.status:
                obj.status = status
            # Disallow issuer change
            if issuer_name and obj.issuer.name != issuer_name:
                raise ReconcileError(obj, "issuer_name")
            if (
                issuer_domicile_code
                and obj.issuer.domicile.country_code != issuer_domicile_code
            ):
                raise ReconcileError(obj, "issuer_domicile_code")

        return obj

    @classmethod
    def update_all(cls, session):
        """Update/create Listed instances and their trade time-series data.

        Updates time series for only listed securities ignoring de-listed ones.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        """
        # Update Listed instances metadata as per the latest data from the data source.
        cls.update_meta_data(session)

        # Only update time series for listed securities ignoring de-listed ones.
        asset_list = session.query(cls).filter(cls.status == "listed").all()

        # Get EOD trade data for this Listed subclass.
        cls.update_eod_time_series(session, asset_list)

    @classmethod
    def dump(cls, session, dumper: Dump):
        """Dump ``Listed`` metadata and its time series to disk.

        This method serialises all ``Listed`` instances (identified by ISIN)
        to a :class:`pandas.DataFrame` and writes it via ``dumper`` under the
        key ``Listed.__name__``. It then delegates to ``ListedEOD.dump`` to
        persist end-of-day time-series data for each listed security.

        Only data that originates from external data sources (such as APIs)
        is dumped. Static reference tables (currencies, domiciles, exchanges,
        issuers, etc.) are *not* dumped and must be recreated separately when
        reusing.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        dumper : .financial_data.Dump
            The financial data dumper.

        See also
        --------
        .reuse
        .asset_base.AssetBase.dump

        """
        # Dump all listed security meta-data
        dump_dict = dict()
        dump_dict[cls.__name__] = cls.to_data_frame(session)
        # Serialize
        dumper.write(dump_dict)

        # Dump all security end-of-day time-series data
        ListedEOD.dump(session, cls, dumper)

    @classmethod
    def reuse(cls, session, dumper: Dump):
        """Populate ``Listed`` and its EOD series from a dump.

        This reads previously dumped ``Listed`` metadata (keyed by ISIN) from
        ``dumper`` and reconstructs or updates instances via
        :meth:`from_data_frame`. It then delegates to ``ListedEOD.reuse`` to
        restore end-of-day time-series data for each listed security.

        Intended usage
        --------------
        - Use this primarily to initialise a *new* and otherwise empty
            application database that already contains all required static
            reference data (currencies, domiciles, exchanges, issuers, etc.).
        - The dump is keyed by business identifiers (ISIN and MIC/ticker). New
            database primary keys are allowed to differ from the database that
            produced the dump.

        Effects on existing data
        ------------------------
        - When called on a non-empty database, existing ``Listed`` instances
            for the same identifiers may be reconciled or updated by the
            underlying :meth:`factory` / :meth:`from_data_frame` logic.
        - Time-series reuse deletes existing records for the relevant asset
            class before inserting those from the dump (see
            ``TimeSeriesBase.reuse``).

        Note that there is currently no date-based filtering: all rows present
        in the dump will be reinserted for the matching assets.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        dumper : .financial_data.Dump
            The financial data dumper.

        See also
        --------
        .dump

        """
        # Re-use all listed security meta-data
        class_name = cls.__name__
        data_frame_dict = dumper.read(key_name_list=[class_name])
        cls.from_data_frame(session, data_frame_dict[class_name])
        logger.warning(
            "Reused %s data - Data may be stale. Consider a hard "
            "reinitialisation if security mete-data has changed.", class_name)

        # Re-use all security end-of-day time-series data
        ListedEOD.reuse(session, cls, dumper)


class ListedEquity(Listed):
    # TODO: Make a ListedEquityBase parent with Equity next to ETF child classes. The idea is to use only the leaves orf a hierarchical tree
    """Exchange listed ordinary shares in an issuing company.

    Ordinary shares are also known as equity shares and they are the most
    common form of share in the UK. An ordinary share gives the right to its
    owner to share in the profits of the company (dividends) and to vote at
    general meetings of the company. The residual value of the company is
    called common stock. A voting share (also called common stock or an
    ordinary share) is a share of stock giving the stockholder the right to
    vote on matters of corporate policy and the composition of the members of
    the board of directors.

    Parameters
    ----------
    name : str
        Entity full name.
    issuer: .Issuer
        The issuing institution that issues the asset for exchange.
    isin : str
        An International Securities Identification Number (ISIN) uniquely
        identifies a security. ISINs consist of two alphabetic characters, which
        are the ISO 3166-1 alpha-2 code for the issuing country (An ISIN cannot
        specify a particular trading location.), nine alpha-numeric characters
        (the National Securities Identifying Number, or ISIN, which identifies
        the security, padded as necessary with leading zeros), and one numerical
        check digit.
    exchange : .Exchange
        The exchange the asset is listed upon.
    ticker : str
        The ticker assigned to the asset by the exchange listing process.
    status : str
        Flag of listing status ('listed', 'delisted').
    quote_units : {'units', 'cents'}, optional
        Price quotations are either in currency units (default) or currency
        cents.
    shares_in_issue : int, optional
        Number of shares in issue.
    date_stamp : datetime.date
        Date stamp of the data.
    industry_class : str, optional
        The class' mnemonic for the industry classification scheme. The valid
        values are:

            * 'icb' for the Industry Classification Benchmark (ICB)
            * No others are implemented as yet.

    industry_class_dict : dict, required if industry_class is given
        A dictionary of the complete parameter set accepted the industry
        classification class which has been selected for by the
        ``industry_class`` parameter. See the documentation for the selected
        class.

    See also
    --------
    .Listed, .Issuer, .Exchange
    """

    __tablename__ = "listed_equity"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    # Major asset class constant. Possibly be overridden by child classes.
    _asset_class = "equity"

    _id = Column(Integer, ForeignKey("listed._id"), primary_key=True)
    """ Primary key."""

    # Industry classification
    industry_class = Column(String(16), nullable=True)
    """str: The `industry classification`_ mnemonic in lowercase.

    .. _`industry classification`:
        https://en.wikipedia.org/wiki/Industry_classification

    """
    # Industry classification foreign keys. This is backref'ed as
    # industry_class_icb
    _industry_class_icb_id = Column(
        Integer, ForeignKey("industry_class_icb._id"), nullable=True
    )

    # The financial_data METADATA_GET_METHOD method should be overridden here
    # but as its not yet implemented there is just a comment.
    METADATA_GET_METHOD = None

    # Associated EOD time-series class override for this asset class.
    TIME_SERIES_CLASS = ListedEquityEOD

    # The Dividend and Split getter methods
    DIVIDEND_GET_METHOD = AssetBase.HISTORY_INSTANCE.get_dividends
    SPLIT_GET_METHOD = AssetBase.HISTORY_INSTANCE.get_splits

    # Associated Dividend & Split time-series class for this asset
    # class.
    DIVIDEND_TIME_SERIES_CLASS = Dividend
    SPLIT_TIME_SERIES_CLASS = Split


    # FIXME: The __repr__ string is printing Currency.__str__ instead of
    # Currency.__repr__

    def __init__(self, name, issuer, isin, exchange, ticker, status, **kwargs):
        """Instance initialization."""

        # Select industry classification scheme, initialise and add it.
        if "industry_class" in kwargs:
            if kwargs["industry_class"] == "icb":
                self.industry_class = kwargs.pop("industry_class")
                # Create and assign the industry classification instance
                self._industry_class_icb = IndustryClassICB(
                    industry_name=kwargs.pop("industry_name"),
                    super_sector_name=kwargs.pop("super_sector_name"),
                    sector_name=kwargs.pop("sector_name"),
                    sub_sector_name=kwargs.pop("sub_sector_name"),
                    industry_code=kwargs.pop("industry_code"),
                    super_sector_code=kwargs.pop("super_sector_code"),
                    sector_code=kwargs.pop("sector_code"),
                    sub_sector_code=kwargs.pop("sub_sector_code"),
                )
            else:
                raise ValueError(
                    "The `industry_class` {} is not implemented.".format(
                        self.industry_class
                    )
                )

        super().__init__(name, issuer, isin, exchange, ticker, status, **kwargs)

    def __repr__(self):
        """Return the official string output."""
        return '{}(name="{}", issuer={!r}, isin="{}", exchange={!r}, ticker="{}", status="{}")'.format(
            self.__class__.__name__, self.name, self.issuer, self.isin, self.exchange, self.ticker, self.status
        )

    @property
    def _dividend_series(self):
        return [ts for ts in self._time_series_single_item if isinstance(ts, Dividend)]

    @property
    def _split_series(self):
        return [ts for ts in self._time_series_single_item if isinstance(ts, Split)]

    @property
    def industry_class_instance(self):
        # NOTE: If we use EOD API maybe we don't need ICB
        """The `industry classification`_ instance.

        Is an instance of the class that encodes the industry classification.
        The possible classes are:

        IndustryClassICB
            The Industry Classification Benchmark (ICB)

        No others are implemented yet. See the `industry classification`_
        table.

        .. _`industry classification`:
            https://en.wikipedia.org/wiki/Industry_classification

        """
        if self.industry_class == "icb":
            return self._industry_class_icb
        else:
            pass

    def to_dict(self):
        """Convert class data attributes into a factory compatible dictionary.

        Returns
        -------
        dict
            The dictionary keys are the same as the class' ``factory`` method
            argument names, with the exception of the ``cls``, ``session`` and
            ``create`` arguments.

        """
        dictionary = super().to_dict()

        # Only add industry classification if it exists
        if self._industry_class_icb is not None:
            additional_dict = {
                "industry_class": self.industry_class,
                "industry_name": self._industry_class_icb.industry_name,
                "super_sector_name": self._industry_class_icb.super_sector_name,
                "sector_name": self._industry_class_icb.sector_name,
                "sub_sector_name": self._industry_class_icb.sub_sector_name,
                "industry_code": self._industry_class_icb.industry_code,
                "super_sector_code": self._industry_class_icb.super_sector_code,
                "sector_code": self._industry_class_icb.sector_code,
                "sub_sector_code": self._industry_class_icb.sub_sector_code,
            }
            dictionary.update(additional_dict)

        return dictionary

    @classmethod
    def update_corporate_time_series(cls, session, asset_list):
        """Update the Dividend and Split data of all the instances.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        asset_list : list of Asset or child class instances
            The list of asset instances to update the EOD time-series for.

        This is an override of the parent class method to add the update of
        Dividend and Split time-series data. The parent class method is called
        to update the EOD time-series data.

        """

        # Bulk add/update Dividend time-series data uses the time-series class
        # factory method.
        dividend_data_frame = cls.DIVIDEND_GET_METHOD(asset_list)
        cls.DIVIDEND_TIME_SERIES_CLASS.from_data_frame(session, cls, dividend_data_frame)

        # Bulk add/update Split time-series data uses the time-series class
        # factory method.
        split_data_frame = cls.SPLIT_GET_METHOD(asset_list)
        cls.SPLIT_TIME_SERIES_CLASS.from_data_frame(session, cls, split_data_frame)

    @classmethod
    def update_all(cls, session):
        """Update/create all ListedEquity securities and their time series.

        Updates time series for only listed securities ignoring de-listed ones.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        """

        # The superclasses will get securities metadata and EOD trade data for
        # this ListedEquity subclass
        super().update_all(session)

        # Only update time series for listed securities ignoring de-listed ones.
        asset_list = session.query(cls).filter(cls.status == "listed").all()

        # Get Dividend and Split time-series data.
        cls.update_corporate_time_series(session, asset_list)

    @classmethod
    def dump(cls, session, dumper: Dump):
        """Dump all class instances and their time series data to disk.

        The data can be re-used to re-create all class instances and the time
        series data using the ``reuse`` method.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        dumper : .financial_data.Dump
            The financial data dumper.

        See also
        --------
        .asset_base.AssetBase.dump

        """
        # Parent dumper
        super().dump(session, dumper)

        # Dump all security dividend and split time-series data
        Dividend.dump(session, cls, dumper)
        Split.dump(session, cls, dumper)

    @classmethod
    def reuse(cls, session, dumper: Dump):
        """Reuse dumped data as a database initialization resource.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        dumper : .financial_data.Dump
            The financial data dumper.

        Warning
        -------
        This method is intended to be used only to initialise a new and empty database.
        Data in the reused dump file that has a `date_stamp` on or before the
        recorded last date from the previous addition of time series instances
        will be ignored. In other words, your dumped data will not be reused.

        See also
        --------
        .dump

        """
        # Parent class re-user
        super().reuse(session, dumper)

        # Re-use all security dividend and split time-series data
        Dividend.reuse(session, cls, dumper)
        Split.reuse(session, cls, dumper)

    def get_dividend_series(self):
        """Return the dividends data series for the security.

        Returns
        -------
        pandas.DataFrame
            A dividend ``pandas.DataFrame`` with columns identical to the keys
            from the ``time_series.Dividend.to_dict()`` polymorph class method.

        Raises
        ------
        DividendSeriesNoData
            If no time series exists.
        """
        # Use direct SQL query for better performance than ORM
        from sqlalchemy import text
        session = object_session(self)

        query = text("""
        SELECT
            tsb.date_stamp,
            d.currency,
            d.declaration_date,
            d.payment_date,
            d.period,
            d.record_date,
            d.unadjusted_value,
            d.adjusted_value
        FROM time_series_base tsb
        INNER JOIN dividend d ON tsb._id = d._id
        WHERE tsb._asset_id = :asset_id
            AND tsb._discriminator = 'dividend'
        ORDER BY tsb.date_stamp ASC
        """)

        series = pd.read_sql(query, session.bind, params={'asset_id': self._id})

        # If no dividend records exist
        if len(series) == 0:
            # If distributions are expected then raise an explicit exception
            # so that callers can decide how to handle missing data.
            if self.distributions:
                raise DividendSeriesNoData(
                    f"Expected dividend data for {self.identity_code} as `distributions` attribute is True.")

            # If distributions are not expected and no data is present then
            # return an empty DataFrame with the minimal required columns so
            # that downstream code can treat dividends as simply absent.
            series = pd.DataFrame(columns=["date_stamp", "unadjusted_value"])
            series["date_stamp"] = pd.to_datetime(series["date_stamp"])
            series.set_index("date_stamp", inplace=True)
            series.sort_index(inplace=True)
            series.name = self
            return series

        # Warn if no distributions are expected but data is found. This is not
        # an exception as the distributions attribute may be incorrectly set to
        # False or there may be special distribution events.
        if not self.distributions and len(series) > 0:
            logger.warning(
                f"Found dividend data for {self.identity_code} but `distributions` attribute "
                "is False. Check if `distributions` is correctly set.")

        # Handle quote_units conversion for dividend values
        if self.quote_units == "cents":
            for col in ['unadjusted_value', 'adjusted_value']:
                if col in series.columns and series[col].notna().any():
                    series[col] = series[col] / 100.0

        series["date_stamp"] = pd.to_datetime(series["date_stamp"])
        series.set_index("date_stamp", inplace=True)
        series.sort_index(inplace=True)  # Assure ascending
        series.name = self

        return series

    def get_last_dividend(self):
        """Return the dividend last date for the listed asset.

        Returns
        -------
        .time_series.Dividend or polymorph child class
            The last ``.time_series.Dividend`` (or child class) time series
            instance.

        Raises
        ------
        DividendSeriesNoData
            If no time series exists.
        """
        if len(self._dividend_series) == 0:
            raise DividendSeriesNoData(f"Expected dividend data for {self.identity_code}")
        else:
            return self._dividend_series[-1]

    def get_last_dividend_date(self):
        """Return the last dividend date for the listed asset.

        Returns
        -------
        datetime.date
            The date of the last dividend for the listed asset.

        Raises
        ------
        DividendSeriesNoData
            If no time series exists.
        """
        last_dividend = self.get_last_dividend()
        return last_dividend.date_stamp

    def get_split_series(self):
        """Return the splits data series for the security.

        Returns
        -------
        pandas.DataFrame
            A split ``pandas.DataFrame`` with columns identical to the keys
            from the ``time_series.Split.to_dict()`` polymorph class method.

        Raises
        ------
        SplitSeriesNoData
            If no time series exists.
        """
        # Use direct SQL query for better performance than ORM
        from sqlalchemy import text
        session = object_session(self)

        query = text("""
        SELECT
            tsb.date_stamp,
            s.numerator,
            s.denominator
        FROM time_series_base tsb
        INNER JOIN split s ON tsb._id = s._id
        WHERE tsb._asset_id = :asset_id
            AND tsb._discriminator = 'split'
        ORDER BY tsb.date_stamp ASC
        """)

        series = pd.read_sql(query, session.bind, params={'asset_id': self._id})

        if len(series) == 0:
            raise SplitSeriesNoData(f"Expected split data for {self.identity_code}")

        series["date_stamp"] = pd.to_datetime(series["date_stamp"])
        series.set_index("date_stamp", inplace=True)
        series.sort_index(inplace=True)  # Assure ascending
        series.name = self

        return series

    def get_last_split(self):
        """Return the split last date for the listed asset.

        Returns
        -------
        .time_series.Split or polymorph child class
            The last ``.time_series.Split`` (or child class) time series
            instance.

        Raises
        ------
        SplitSeriesNoData
            If no time series exists.
        """
        if len(self._split_series) == 0:
            raise SplitSeriesNoData(f"Expected split data for {self.identity_code}")
        else:
            return self._split_series[-1]

    def get_last_split_date(self):
        """Return the last split date for the listed asset.

        Returns
        -------
        datetime.date
            The date of the last split for the listed asset.

        Raises
        ------
        SplitSeriesNoData
            If no time series exists.
        """
        last_split = self.get_last_split()
        return last_split.date_stamp

    def get_time_series_processor(self, price_item="close"):
        """Return a TimeSeriesProcessor for this asset.

        Parameters
        ----------
        price_item : str, optional
            The specific item of price such as 'close', 'open', `high`, or
            `low`. The selected price item will be renamed to "price" for the
            processor. The default is 'close'.

        Returns
        -------
        .time_series_processor.TimeSeriesProcessor
            A ``.time_series_processor.TimeSeriesProcessor`` instance for this
            asset with the end-of-day prices, dividends, and splits data
            series. The `identity` column is set to this
            ``ListedEquity`` instance.

        Raises
        ------
        ValueError
            If the ``price_item`` argument is not a column in the end-of-day
            price series.
        """
        # Get EOD prices and do not handle the EODSeriesNoData exception here as
        # the processor cannot be created without price data. The exception
        # should be handled by the caller.
        eod = self.get_eod_series()
        # Check price item is valid
        if price_item not in eod.columns:
            raise ValueError(
                f"Unexpected `price_item` argument {price_item}. "
                f"Expected one of {list(eod.columns)}.")

        # Get prices, select price item, and rename to "price" for the processor.
        prices_df = eod.reset_index()
        prices_df["identity"] = self
        columns_to_keep = ["identity", "date_stamp", price_item]
        prices_df = prices_df[columns_to_keep]
        columns_to_rename = {price_item: "price"}
        prices_df.rename(columns=columns_to_rename, inplace=True)

        # Get dividends, select dividends unadjusted value item, and rename to
        # "dividend" for the processor.
        try:
            dividends_df = self.get_dividend_series().reset_index()
        except DividendSeriesNoData:
            logger.warning(
                f"No dividend data for {self.identity_code}. "
                "Distributions were expected but Dividend series will be empty.")
            dividends_df = None
        else:
            dividends_df["identity"] = self
            columns_to_keep = ["identity", "date_stamp", "unadjusted_value"]
            dividends_df = dividends_df[columns_to_keep]
            # Add dividend column as a copy of unadjusted_value
            dividends_df["dividend"] = dividends_df["unadjusted_value"]

        # Get splits
        try:
            splits_df = self.get_split_series().reset_index()
        except SplitSeriesNoData:
            splits_df = None
        else:
            splits_df["identity"] = self
            columns_to_keep = ["identity", "date_stamp", "numerator", "denominator"]
            splits_df = splits_df[columns_to_keep]

        # Create and return the processor
        tsp = TimeSeriesProcessor(prices_df, dividends_df, splits_df)

        return tsp


class Index(AssetBase):
    # TODO: Should be a child of Common as it isn'st an asset per se.
    """An index representing some financial data.

    Wikipedia defines an index is an indirect short-cut derived from and
    pointing into, a greater volume of values, data, information or knowledge.

    In the case of this class the index is usually a financial index but this
    need not be so.

    A statistical measure of change in an economy or a securities market. In
    the case of financial markets, an index is an imaginary portfolio of
    securities representing a particular market or a portion of it. Each index
    has its own calculation methodology and is usually expressed in terms of a
    change from a base value. Thus, the percentage change is more important
    than the actual numeric value.

    This may also be an indice of economic importance such as the normalized
    price of a consumer commodities basket for representing inflation or it may
    be a population of a country or a GDP, etc.

    The Standard & Poor's 500 is one of the world's best known indexes, and is
    the most commonly used benchmark for the stock market. Other prominent
    indexes include the DJ Wilshire 5000 (total stock market), the MSCI EAFE
    (foreign stocks in Europe, Australasia, Far East) and the Lehman Brothers
    Aggregate Bond Index (total bond market).

    Because, technically, you can't actually invest in an index, index mutual
    funds and exchange- traded funds (based on indexes) allow investors to
    invest in securities representing broad market segments and/or the total
    market.

    It is important to realize that each index must have an issuer institution.

    Parameters
    ----------
    name : str
        Index full name.
    ticker : str
        A short mnemonic code (often derived from the name) used to identity
        the index. This may be used in conjunction with the issuer name or
        issuer code (or ticker) to uniquely identity the index in the world.
    currency : .entity.Currency
        Currency of asset pricing.
    total_return : bool, optional
        Indicates the index time series is a total return price series.
    static : bool, optional
        If set True then the index's time series data is static, i.e., not
        updated and so would be ignored when the ``Index.update_all`` method is
        called.



    See also
    --------
    .Entity, .Institution,

    """

    __tablename__ = "index"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    _id = Column(Integer, ForeignKey("asset_base._id"), primary_key=True)

    KEY_CODE_LABEL = "ticker"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    # Unique index ticker
    ticker = Column(String(12), nullable=False)

    # Ticker must be unique
    __table_args__ = (UniqueConstraint("ticker"),)

    # Indicates the index time series is a total return price series
    total_return = Column(Boolean, nullable=False)

    # If set True then the index's time series data is static, i.e., not updated
    # and so would be ignored when the ``IndexEOD.update_all`` method is called.
    static = Column(Boolean, nullable=False)

    # The financial_data EOD_GET_METHOD method overridden here
    EOD_GET_METHOD = AssetBase.HISTORY_INSTANCE.get_indices_eod

    def __init__(
        self, name, ticker, currency, total_return=False, static=False, **kwargs
    ):
        """Instance initialization."""
        self.ticker = ticker
        self.total_return = total_return
        self.static = static

        super().__init__(name, currency, **kwargs)

        # TODO: MAke sure Index tickers are unique across the world. This is a
        # big assumption but it is necessary for the key_code to be just the
        # ticker.

    def __repr__(self):
        """Return the official string output."""
        return '{}(name="{}", ticker="{}", currency={!r}, total_return={!r}, static={!r})'.format(
            self.__class__.__name__, self.name, self.ticker, self.currency, self.total_return, self.static
        )

    def _get_identity_code(self):
        """Required for unique identification of instances and is not optional."""
        return self.ticker

    @property
    def key_code(self):
        """Return a unique string code for this class instance."""
        return f"{self.ticker}"

    @property
    def long_name(self):
        """str: Return the long name string."""
        msg = "{} is an {} priced in {}.".format(
            self.name, self.__class__.__name__, self.currency.ticker
        )

        return msg

    @classmethod
    def factory(cls, session, index_name, ticker, currency_code, create=True, **kwargs):
        """Manufacture/retrieve an instance from the given parameters.

        If a record of the specified class instance does not exist then add it,
        else do nothing. Then return the instance.

        Factory Method Behaviour
        ------------------------
        This factory operates in two modes controlled by the ``create`` parameter:

        **Retrieval Mode** (create=False):
            Retrieves an existing Index by ticker. Raises ``FactoryError`` if not
            found.

            Example::

                # Must already exist in database
                index = Index.factory(
                    session, index_name="S&P 500", ticker="^GSPC",
                    currency_code="USD", create=False
                )

        **Creation Mode** (create=True, default):
            Retrieves existing Index or creates new one if missing. **Important**:
            The specified currency must already exist or ``FactoryError`` is raised.

            Example::

                # Currency "USD" must already exist
                index = Index.factory(
                    session, index_name="S&P 500", ticker="^GSPC",
                    currency_code="USD"
                )

        **Dependency Enforcement**:
            This factory calls ``Currency.factory(session, currency_code)`` in
            retrieval mode (without name or country_code_list), ensuring the
            currency must pre-exist. This prevents accidental creation of Currency
            records.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        index_name : str
            Entity full name. Required for both retrieval and creation.
        ticker : str
            A short mnemonic code (often derived from the name) used to identify
            the index. This may be used in conjunction with the issuer to
            uniquely identify the index in the world.
        currency_code : str(3)
            ISO 4217 3-letter currency code. The currency must already exist in
            the database.
        create : bool, optional
            If False, raises ``FactoryError`` if index doesn't exist. If True
            (default), creates index if missing. Default is True.
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        Index
            The single instance that is in the session.

        Raises
        ------
        FactoryError
            If index not found when create=False, or if specified currency
            doesn't exist.

        See Also
        --------
        Currency.factory : Called in retrieval mode to get currency

        """
        # Some indices such as "Crypto Volatility Index" have unknown currency
        if currency_code in ("Unknown"):
            currency_code = "ZZZ"
        currency = Currency.factory(session, currency_code)

        # Check if exchange exists in the session and if not then add it.
        try:
            obj = session.query(cls).filter(cls.ticker == ticker).one()
        except NoResultFound:
            if not create:
                raise FactoryError(
                    'Index "{}" with ticker="{}", not found.'.format(index_name, ticker)
                )
            else:
                # Create and add.
                obj = cls(index_name, ticker, currency)
                session.add(obj)
        else:
            # Changes are not allowed so nothing to reconcile here.
            pass

        return obj

    @classmethod
    def update_all(cls, session):
        """Update/create all Index securities and their EOD data.


        This is currently a placeholder method to be implemented in future
        development work.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        """
        # TODO: Implement index get metadata and EOD data retrieval and updating.
        raise NotImplementedError("Index.update_all is not implemented yet.")


class ExchangeTradeFund(ListedEquity):
    """An exchange-traded fund (ETF).

    An exchange-traded fund (ETF) is an investment fund traded on stock
    exchanges, much like stocks. An ETF holds assets such as stocks,
    commodities, or bonds, and trades close to its net asset value over the
    course of the trading day. Most ETFs track an index, such as a stock index
    or bond index. ETFs may be attractive as investments because of their low
    costs, tax efficiency, and stock-like features. By 2013, ETFs were the
    most popular type of exchange-traded product.

    An ETF combines the valuation feature of a mutual fund or unit investment
    trust, which can be bought or sold at the end of each trading day for its
    net asset value, with the trade-ability feature of a closed-end fund, which
    trades throughout the trading day at prices that may be more or less than
    its net asset value. Closed-end funds are not considered to be ETFs, even
    though they are funds and are traded on an exchange.

    ETFs offer both tax efficiency and lower transaction costs.


    Parameters
    ----------
    domicile : .Domicile
        Domicile of the entity.
    name : str
        Entity full name.
    issuer: .Issuer
        The issuing institution that issues the asset for exchange.
    isin : str
        An International Securities Identification Number (ISIN) uniquely
        identifies a security. ISINs consist of two alphabetic characters, which
        are the ISO 3166-1 alpha-2 code for the issuing country (An ISIN cannot
        specify a particular trading location.), nine alpha-numeric characters
        (the National Securities Identifying Number, or ISIN, which identifies
        the security, padded as necessary with leading zeros), and one numerical
        check digit.
    exchange : .Exchange
        The exchange the asset is listed upon.
    ticker : str
        The ticker assigned to the asset by the exchange listing process.
    status : str
        Flag of listing status ('listed', 'delisted').
    industry_class : str
        The class' mnemonic for the industry classification scheme. The valid
        values are:

            * 'icb' for the Industry Classification Benchmark (ICB)
            * No others are implemented as yet.

    industry_class_dict : dict
        A dictionary of the complete parameter set accepted the industry
        classification class which has been selected for by the
        ``industry_class`` parameter. See the documentation for the selected
        class.
    asset_class : str
        The major asset class. May be one of 'money', 'bond', 'property',
        'equity', 'commodity'.
    locality : str
        May take on the values 'domestic' or 'foreign'.

    Note
    ----
    The ``asset_class`` and ``locality``` parameters are are workarounds for not
    having data for all the underlying securities for our ETFs in the session.
    Remedying this shall  be the focus of future development work.

    """

    __tablename__ = "exchange_traded_fund"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    _id = Column(Integer, ForeignKey("listed_equity._id"), primary_key=True)
    """ Primary key."""

    # The index, if any, that the ETF attempts to replicate.
    index = Column(Integer, ForeignKey("index._id"), nullable=True)

    # HACK: These are are workarounds for not having data for all the underlying
    # securities for our ETFs.
    _classes = ASSET_CLASSES
    _asset_class = Column(Enum(*_classes))
    _locality = Column(String)

    # Published Total Expense Ratio of the fund.
    ter = Column(Float, nullable=True)

    # The financial_data METADATA_GET_METHOD method is overridden here
    METADATA_GET_METHOD = AssetBase.METADATA_INSTANCE.get_etfs_meta

    def __init__(self, name, issuer, isin, exchange, ticker, status, **kwargs):
        """Instance initialization."""

        # Optional parameters.
        if "index" in kwargs:
            self.index = kwargs.pop("index")
        if "asset_class" in kwargs:
            self._asset_class = kwargs.pop("asset_class")
        if "locality" in kwargs:
            self._locality = kwargs.pop("locality")
        if "ter" in kwargs:
            self.ter = kwargs.pop("ter")
            if self.ter == "":
                self.ter = float("nan")
        else:  # Default to zero.
            self.ter = float("nan")

        super().__init__(name, issuer, isin, exchange, ticker, status, **kwargs)

    def __repr__(self):
        """Return the official string output."""
        return '{}(name="{}", issuer={!r}, isin="{}", exchange={!r}, ticker="{}", status="{}")'.format(
            self.__class__.__name__, self.name, self.issuer, self.isin, self.exchange, self.ticker, self.status
        )

    def get_locality(self, domicile_code):
        """Return the locality "domestic" or "foreign".

        Parameters
        ----------
        domicile_code : str(2)
            ISO 3166-1 Alpha-2 two letter country code of the current account's
            domicile. The asset's domestic or foreign status shall be determined
            relative to this.

        Return
        ------
        str
            The asset's domestic or foreign status relative to the current
            account domicile. See above examples.

        The "domestic" or "foreign" status of an exchange traded fund in a
        current account is determined only if all the underlying securities of
        the fund are of the same locality as defined in the
        ``Listed.get_locality`` method:

        'domestic':
            If the domicile code of the current account and the fund's
            underlying securities domicile codes all agree.
        'foreign':
            If the domicile code of the current account and the fund's
            underlying securities domicile codes all disagree.

        Note
        ----
        Currently the "domestic" or "foreign" locality status is determined by
        external information entered into the asset_base as we don't currently
        carry information regarding the underlying securities.

        """
        if self._locality == domicile_code:
            locality = "domestic"
        else:
            locality = "foreign"

        return locality
