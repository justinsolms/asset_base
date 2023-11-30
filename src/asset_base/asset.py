#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Define classes describing assets such as financial assets.
"""

# TODO: Decide upon key_code and identity_code formats

import sys
import functools
import numpy as np
import pandas as pd

import stdnum.isin as stdisin

from numpy import abs
from scipy.signal import filtfilt

from sqlalchemy import Float, Integer, String, Enum, Boolean
from sqlalchemy import MetaData, Column, ForeignKey

from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound

from asset_base.exceptions import FactoryError, EODSeriesNoData, DividendSeriesNoData
from asset_base.exceptions import ReconcileError
from asset_base.exceptions import BadISIN
from asset_base.financial_data import Dump
from asset_base.entity import Currency, Exchange, Issuer
from asset_base.common import Common
from asset_base.industry_class import IndustryClassICB
from asset_base.time_series import (
    Dividend,
    ForexEOD,
    IndexEOD,
    ListedEOD,
    TimeSeriesBase,
)

# Get module-named logger.
import logging


logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Pull in the meta data
metadata = MetaData()


@functools.total_ordering
class AssetBase(Common):
    """Base class for the module.

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

    id = Column(Integer, ForeignKey("common.id"), primary_key=True)
    """ Primary key."""

    # Asset currency. Optional.
    _currency_id = Column(Integer, ForeignKey("currency.id"), nullable=False)
    currency = relationship(Currency)

    # Price quote in cents or units. Strictly convert all prices to currency
    # units in case of this attribute being in cents.
    quote_units = Column(Enum("units", "cents"), nullable=False)

    # All historical time-series collection ranked by date_stamp
    _series = relationship(
        TimeSeriesBase, order_by=TimeSeriesBase.date_stamp, back_populates="base_obj"
    )
    """list: EOD historical time-series collection ranked by date_stamp

    A list of ``time_series.TimeSeriesBase`` instances.

    Warning
    -------
    As this is an abstract class please do not directly use this attribute.

    See also
    --------
    _eod_series
    """

    def __init__(self, name, currency, **kwargs):
        """Instance initialization."""
        super().__init__(name, **kwargs)

        self.currency = currency

        if "quote_units" in kwargs:
            self.quote_units = kwargs.pop("quote_units")
        else:
            self.quote_units = "units"

    def __str__(self):
        """Return the informal string output. Currently ``identity_code``."""
        return self.identity_code

    def __repr__(self):
        """Return the official string output."""
        return '{}(name="{}", currency={!r})'.format(
            self._class_name, self.name, self.currency
        )

    def __lt__(self, other):
        """Use primarily key ``id`` for sorting. (See Note in class docstring)."""
        return self.id < other.id

    @property
    def long_name(self):
        """str: Return the long name string."""
        return "{} is an {} priced in {}.".format(
            self.name, self._class_name, self.currency_ticker
        )

    @property
    def _eod_series(self):
        """Alias for ``_series`` column attribute.

        Note
        ----
        This MUST be overloaded by an actual ``_eod_series``
        ``sqlalchemy.Column`` column attribute in child polymorphs for their
        proper time-series functionality. Here this alias is merely a
        convenience so we can put the methods ``get_eod``, ``get_last_eod`` and
        ``get_last_eod_date`` in this class. They all use ``_eod_series``. Must
        return a list of polymorphs of ``time_series.TimeSeriesBase`` instances.

        See also
        --------
        _series

        TODO: consider just refactoring _series as _eod_series and test.
        """
        return self._series

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.currency.ticker + "." + self.name

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.currency.ticker + "." + self.name

    @property
    def currency_ticker(self):
        """ISO 4217 3-letter currency code."""
        return self.currency.ticker

    def get_eod(self):
        """Return the EOD time series for the asset.

        # TODO: Rename to `get_eod_series`

        Returns
        -------
        pandas.DataFrame
            An End-Of-Day (EOD) ``pandas.DataFrame`` with columns identical to
            the keys from the ``time_series.SimpleEOD.to_dict()`` or
            ``time_series.ListedEOD.to_dict()`` or polymorph class method.

        Raises
        ------
        EODSeriesNoData
            If no time series exists.
        """
        trade_eod_dict_list = [s.to_dict() for s in self._eod_series]
        if len(trade_eod_dict_list) == 0:
            raise EODSeriesNoData(f"Expected EOD data for {self}.")
        data_frame = pd.DataFrame(trade_eod_dict_list)
        data_frame["date_stamp"] = pd.to_datetime(data_frame["date_stamp"])
        data_frame.set_index("date_stamp", inplace=True)
        data_frame.sort_index(inplace=True)  # Assure ascending
        data_frame.name = self

        return data_frame

    def _get_last_eod(self):
        """Helper method.

        Raises
        ------
        EODSeriesNoData
            If no time series exists.
        """
        try:
            last_eod = self._eod_series[-1]
        except IndexError:
            raise EODSeriesNoData(f"Expected EOD data for {self}.")

        return last_eod

    def get_last_eod(self):
        """Return the EOD last date's, data dict, for the asset.

        Returns
        -------
        dict
            An End-Of-Day (EOD) price data dictionary with keys from the
            ``time_series.ListedEOD.to_dict()`` method.

        Raises
        ------
        EODSeriesNoData
            If no time series exists.
        """
        return self._get_last_eod().to_dict()

    def get_last_eod_date(self):
        """Return the EOD last date for the listed share.

        Returns
        -------
        datetime.date or None
            Last date for the ``.time_series.TimeSeriesBase`` (or child class)
            time series. Returns `None` if no data series exists.
        """
        try:
            last_date = self._get_last_eod().date_stamp
        except EODSeriesNoData:
            last_date = None

        return last_date


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
    owner : .entity.Entity
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

    id = Column(Integer, ForeignKey("asset_base.id"), primary_key=True)
    """ Primary key."""

    # Entity owns Asset. Entity has a reference list to many owned Asset named
    # `asset_list`
    # TODO: Currently owner is allowed to be NULL. Make owner compulsory.
    _owner_id = Column(Integer, ForeignKey("entity.id"), nullable=True)
    owner = relationship("Entity", backref="asset_list", foreign_keys=[_owner_id])

    # TODO: This (or child) is were we would add asset fundamental data relationships
    # TODO: This (or child) is were we would add asset book relationships

    # Major asset class. This is a generic class so the asset class is
    # indeterminate.
    _asset_class = None

    def __init__(self, name, currency, **kwargs):
        """Instance initialization."""
        super().__init__(name, currency, **kwargs)

        # Asset owner
        if "owner" in kwargs:
            self.owner = kwargs.pop("owner")

    def __repr__(self):
        """Return the official string output."""
        if self.owner is None:
            msg = super().__repr__()
        else:
            msg = '{}(name="{}", currency={!r}, owner={!r})'.format(
                self._class_name, self.name, self.currency, self.owner
            )

        return msg

    @property
    def long_name(self):
        """str: Return the long name string."""
        msg = super().__str__()
        if self.owner is not None:
            msg += " Owner: {}".format(self.owner)

        return msg

    @property
    def domicile(self):
        """.entity.Domicile : ``Domicile`` of the ``Share`` owner ``Entity``."""
        # TODO: Currently owner is allowed to be NULL. Make owner compulsory.
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
        'bonds':
            Debt instruments where the owner of the debts is owed by the issuer.
        'property':
            Investments into real-estate. Your home or investment property, plus
            shares of funds that invest in commercial real estate.
        'equity':
            Also called stocks. Shares in publicly held companies or company
            legal vehicles.
        'commodities':
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

    @classmethod
    def factory(cls, session, asset_name, currency_code, create=True, **kwargs):
        """Manufacture/retrieve an instance from the given parameters.

        If a record of the specified class instance does not exist then add it,
        else do nothing. Then return the instance.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        asset_name : str
            Asset full name.
        currency_code : str(3)
            ISO 4217 3-letter currency codes.
        create : bool, optional
            If `False` then the factory shall expect the specified `Entity` to
            already exist in the session or it shall raise an exception instead
            of creating a first instance.

        Note
        ----
        The entity's domicile (and by implication the related currency) must
        already exist in the session or an exception shall be raised.


        Return
        ------
        .Entity
            The single instance that is in the session.

        See also
        --------
        .Domicile.factory

        """
        # TODO: It is debatable whether or not this factory method should exist
        # as Entity should ber an abstract class. Check if entity exists in the
        # session and if not then add it.
        try:
            obj = (
                session.query(cls)
                .join(Currency)
                .filter(cls.name == asset_name, Currency.ticker == currency_code)
                .one()
            )
        except NoResultFound:
            if not create:
                raise FactoryError(
                    'Asset "{}", currency="{}", not found.'.format(
                        asset_name, currency_code
                    )
                )
            else:
                # Create a new instance, fetch pre-existing currency
                currency = Currency.factory(session, currency_code)
                obj = cls(asset_name, currency, **kwargs)
                session.add(obj)
        else:
            # No changes to reconcile, country_code and entity_name are the key
            # arguments.
            pass

        return obj


class Cash(Asset):
    """Cash in currency held.
    # TODO: Integrate with EOD API

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

    Note
    ----
    The currency is constrained to that of the the domicile. Therefore the
    currency ticker and name are that of the currency of the domicile.

    Note
    ----
    Cash may not hold other assets and this class' holding capability has been
    disabled.

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

    id = Column(Integer, ForeignKey("asset.id"), primary_key=True)

    key_code_name = "asset_currency"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    _asset_class = "cash"

    #  A short class name for use in naming
    _name_appendix = "Cash"

    def __init__(self, currency, **kwargs):
        """Instance initialization."""

        # Force ownership of currencies
        assert "owner" not in kwargs, "Missing `owner` argument."

        # The name is constrained to that of the currency.
        name = currency.name
        super().__init__(name, currency, **kwargs)

    def __repr__(self):
        """Return the official string output."""
        msg = "{}(currency={!r})".format(self._class_name, self.currency)

        return msg

    @property
    def ticker(self):
        """ISO 4217 3-letter currency code."""
        return self.currency.ticker

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.ticker

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.ticker

    @property
    def long_name(self):
        """str: Return the long name string."""
        msg = "{} is an {} priced in {}.".format(
            self.name, self._class_name, self.currency_ticker
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

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        ticker : str(3)
            ISO 4217 3-letter currency codes.
        create : bool, optional
            If `False` then the factory shall expect the specified `Entity` to
            already exist in the session or it shall raise an exception instead
            of creating a first instance.

        Return
        ------
        asset_base.Cash
            The single instance that is in the session.

        See also
        --------
        .Asset.factory,

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
        """Update/create all the objects in the asset_base session.

        The existing records of the ``Currency`` instances in
        the session are used to build a ``Cash`` instance for each
        ``Currency`` instance.

        Warning
        -------
        Please run ``Currency.update_all`` before running this current method to
        avoid an Exception.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.

        """
        # A cash instance for every currency
        currency_list = session.query(Currency).all()
        if len(currency_list) == 0:
            raise Exception(
                "No Currency instances found. " "Please run `Currency.update_all`."
            )
        for currency in currency_list:
            Cash.factory(session, currency.ticker)

    def time_series(self, date_index, identifier="asset"):
        """Retrieve historic time-series for a set of class instances.

        Price time-series for cash is a unity time-series as the price of cash
        is always 1.0 per unit in local currency; and by extension the returns
        series is also a unity series. As the time-series exists, only as a
        notion and not in the database, it is required to have the data and date
        range from other non-cash entities to synthesize the cash price time
        series of correct length for concatenation with such data.

        Parameters
        ----------
        dates : pandas.DatetimeIndex
            The time-series dates of other entities that the ``Cash``
            time-series are to be appended to. Without this parameter it is
            impossible to know in advance how long the cash time-series is
            required to be.
        identifier : str, optional
            By default the column labels of the returned ``pandas.DataFrame``
            are ``asset.Asset`` (or polymorph child instances) provided by in
            the ``asset_list`` argument. With the `identifier` argument one can
            specify if these column labels are to be substituted:

            'asset':
                The default ``asset.Asset`` (or polymorph child instances)
                provided by in the `asset_list` argument.
            'id':
                The database table `id` column entry.
            'ticker':
                The exchange ticker
            'identify_code':
                That which will be returned by the ``asset.Cash.identity_code``
                attribute.
        """
        if not isinstance(date_index, pd.DatetimeIndex):
            raise ValueError("Unexpected date_index argument type.")

        # Make a series with all prices set to 1.0
        series = pd.Series(len(date_index) * [1.0], index=date_index)

        # Add the entity (Cash) as the Series name for later use as column a
        # label in concatenation into a DataFrame
        # TODO: Replace with a match statement
        if identifier == "asset":
            series.name = self
        elif identifier == "id":
            series.name = self.id
        elif identifier == "ticker":
            series.name = self.ticker
        elif identifier == "identity_code":
            series.name = self.identity_code
        else:
            raise ValueError(f"Unexpected `identifier` argument `{identifier}`.")

        return series


class Forex(Cash):
    """Currency exchange rates.

    A currency - the `base_currency` has it's price expressed in the
    `price_currency`. For example: The United Stated Dollar (USD) has its
    price in Japanese Yen (JPY) and in 2022/06/22 the price of 1 USD was 135
    JPY. USD may be considered the primary currency. JPY may be considered the
    secondary currency. As such the code for this exchange rate shall be USDJPY
    and may be read as USD to JPY, i.e., 1 USD to 135 JPY, or 1USD costs 135JPY.

    All stored forex rates will have as their ``base_currency`` be the
    ``root_currency_ticker``. Arbitrary rates will then be calculated off these stored
    rates.

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

    id = Column(Integer, ForeignKey("cash.id"), primary_key=True)

    key_code_name = "ticker"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    _eod_series = relationship(
        ForexEOD, order_by=ForexEOD.date_stamp, back_populates="forex"
    )
    """list: EOD historical time-series collection ranked by date_stamp

    A list of ``time_series.ForexEOD`` instances.
    """

    _asset_class = "forex"

    #  A short class name for use in naming
    _name_appendix = "Forex"

    # Priced currency, or ``base_currency``
    _currency_id2 = Column(Integer, ForeignKey("currency.id"), nullable=False)
    base_currency = relationship(Currency, foreign_keys=[_currency_id2])

    # Currency ticker is redundant information, but very useful and inexpensive
    ticker = Column(String(6))

    # The reference or root ticker. Its price will always be 1.0.
    root_currency_ticker = "USD"

    # List of top foreign currencies. Their time series are maintained as the
    # price of 1 unit of the ``root_currency_ticker``. South African ZAR is included for
    # domestic reasons.
    foreign_currencies = [
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

    def __init__(self, base_currency, price_currency, **kwargs):
        """Instance initialization."""
        # FIXME: For this version assert the ``base_currency`` to be the ``root_currency_ticker`` and state clearly in the documentation

        # The name is constrained to that of the currency.
        # Note that we set the pricing currency of the cash asset here.
        super().__init__(price_currency, **kwargs)
        self.name = f"{base_currency.ticker}{price_currency.ticker}"

        self.currency = price_currency
        self.base_currency = base_currency

        assert (
            base_currency.ticker == self.root_currency_ticker
        ), "Expected the `base_currency` to be the root currency (USD)."

        # Ticker is Joined ISO 4217 3-letter currency codes
        self.ticker = "{}{}".format(self.base_currency.ticker, self.currency.ticker)

    def __repr__(self):
        """Return the official string output."""
        return "{}(base_currency={!r}, price_currency={!r})".format(
            self._class_name, self.base_currency.ticker, self.currency.ticker
        )

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
    def identity_code(self):
        """A human readable string unique to the class instance."""
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

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        base_ticker : str(3)
            ISO 4217 3-letter currency code. The priced or ``base_currency``.
        price_ticker : str(3)
            ISO 4217 3-letter currency code. The price currency.
        create : bool, optional
            If `False` then the factory shall expect the specified `Entity` to
            already exist in the session or it shall raise an exception instead
            of creating a first instance.

        Return
        ------
        asset_base.Cash
            The single instance that is in the session.

        See also
        --------
        .Asset.factory,

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
    def update_all(cls, session, get_forex_method=None, _test_forex_list=None):
        """Update/create all the objects in the asset_base session.

        The existing records of the ``Currency`` instances in
        the session are used to build a ``Cash`` instance for each
        ``Currency`` instance.

        Warning
        -------
        Please run ``Currency.update_all`` before running this current method to
        avoid an Exception.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        get_forex_method : financial_data module class method, optional
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all the ``ForexEOD.factory`` method arguments. This is
            for the securities time series trade end of day data form which the
            ``ForexEOD`` instances shall be created. If this argument is omitted
            then the ``ForexEOD`` time_series will not be created.

        """
        # For testing only
        if _test_forex_list is not None:
            foreign_currencies_list = _test_forex_list
        else:
            foreign_currencies_list = cls.foreign_currencies

        # Create Forex instances as per the Forex.foreign_currencies list
        # attribute
        foreign_currencies = (
            session.query(Currency)
            .filter(
                Currency.ticker.in_(foreign_currencies_list),
            )
            .all()
        )
        if len(foreign_currencies) == 0:
            raise Exception(
                "No Currency instances found. " "Please run `Currency.update_all`."
            )
        if len(foreign_currencies_list) != len(foreign_currencies):
            raise FactoryError("Not all foreign currencies were found.")
        for price_currency in foreign_currencies:
            Forex.factory(session, cls.root_currency_ticker, price_currency.ticker)

        # Get EOD trade data for Forex.
        if get_forex_method is not None:
            ForexEOD.update_all(session, get_forex_method)

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
            eod = forex.get_eod()
            eod_dict[price_ticker] = eod[price_item]
        df_eod_prices = pd.DataFrame(eod_dict)
        # Keep last price over holiday periods
        df_eod_prices.ffill(axis="index", inplace=True)

        # Get the `root_currency_ticker` prices in the desired `base_ticker`
        # currency.
        forex = cls.factory(session, cls.root_currency_ticker, base_ticker)
        eod = forex.get_eod()
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
    FIXME: Args like these are NOT optional
    quote_units : {'units', 'cents'}, optional
        Price quotations are either in currency units (default) or currency
        cents.
    shares_in_issue : int, optional
        Number of shares in issue.

    Attributes
    ----------
    issuer: .Issuer
        The issuing institution that issues the asset for exchange.

    See also
    --------
    .Asset, .Issuer

    """

    # TODO: Create Account class to contain assets with many-to-one relationship

    __tablename__ = "share"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    id = Column(Integer, ForeignKey("asset.id"), primary_key=True)
    """ Primary key."""

    # TODO: Here we would add share unitization and ownership relationships

    # Issuer issues Share. Issuer has a reference list to many issued Share
    # named `share_list`
    _issuer_id = Column(Integer, ForeignKey("issuer.id"), nullable=False)
    issuer = relationship("Issuer", backref="share_list")

    # Number of share units issued byu the Issuer
    shares_in_issue = Column(Integer, nullable=True)

    # Does the share pay distributions or not
    # FIXME: Does not reflect the security meta data correctly
    distributions = Column(Boolean, nullable=False, default=False)

    #  A short class name for use in naming
    _name_appendix = "Share"

    def __init__(self, name, issuer, currency=None, **kwargs):
        """Instance initialization."""
        # If the currency is not provided then the currency is the issuer's
        # domicile's currency
        if currency is None:
            currency = issuer.domicile.currency

        super().__init__(name, currency, **kwargs)

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

    @property
    def domicile(self):
        """.entity.Domicile : Same as that of ``Share`` ``Issuer``."""
        return self.issuer.domicile

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.issuer.key_code + "." + self.name

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.issuer.identity_code + "." + self.name

    @property
    def long_name(self):
        """str: Return the long name string."""
        return "{} is a {} issued by {} in {}.".format(
            self.name,
            self._class_name,
            self.issuer.name,
            self.issuer.domicile.country_name,
        )

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

    Note
    ----
    This class is an abstract class and not meant for direct instantiation, but
    is a base class for all listed or traded shares.

    Note
    ----
    The domicile is constrained to the issuer domicile.

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


    Attributes
    ----------
    exchange : .Exchange
        The exchange the asset is listed upon.
    ticker : str
        The ticker assigned to the asset by the exchange listing process.
    isin : str
        An International Securities Identification Number (ISIN) uniquely
        identifies a security. There is a unique constraint on this attribute.

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

    id = Column(Integer, ForeignKey("share.id"), primary_key=True)
    """ Primary key."""

    # Exchange lists Listed. Exchange has a reference list to many issued Listed
    # named `securities_list`
    _exchange_id = Column(Integer, ForeignKey("exchange.id"), nullable=False)
    exchange = relationship("Exchange", backref="securities_list")

    # EOD historical time-series collection ranked by date_stamp
    _eod_series = relationship(
        ListedEOD, order_by=ListedEOD.date_stamp, back_populates="listed"
    )
    """list: EOD historical time-series collection ranked by date_stamp

    A list of ``time_series.ListedEOD`` instances.
    """

    # Ticker on the listing exchange.
    ticker = Column(String(12), nullable=False)
    # National Securities Identifying Number
    isin = Column(String(12), nullable=False)

    key_code_name = "isin"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    # Listing status.
    status = Column(Enum("listed", "delisted"), nullable=False)

    #  A short class name for use in naming
    # TODO: Automate from class magic attributes.
    _name_appendix = "Listed"

    def __init__(self, name, issuer, isin, exchange, ticker, **kwargs):
        """Instance initialization."""
        # Currency is the exchange listing currency, i.e., the exchange's
        # domicile currency which overwrites the parent class Share issuer's
        # domicile's currency
        currency = exchange.domicile.currency

        super().__init__(name, issuer, currency, **kwargs)

        # Do no remove this code!!. Some methods that use this class (such as
        # factory methods) are able to place arguments with a None value, this
        # circumventing Python's positional-arguments checks. Check manually
        # them here.
        if all([name, issuer, isin, exchange, ticker]):
            pass
        else:
            raise ValueError("Unexpected `None` value for some positional arguments.")

        # Instrument identification and listing
        self.exchange = exchange
        self.ticker = ticker

        # Check to see if the isin number provided is valid. This checks the
        # length and check digit.
        isin = Listed._check_isin(isin)

        # Check issuer domicile against the 1st two ISIN letters (ISO 3166-1
        # alpha-2 code)
        if isin[0:2] == self.issuer.domicile.country_code:
            self.isin = isin
        else:
            raise ValueError("Unexpected domicile. Does not match ISIN country code.")

        # Listing status
        if "status" in kwargs:
            self.status = kwargs.pop("status")
        else:
            self.status = "listed"

    @property
    def domicile(self):
        """.entity.Domicile : ``Domicile`` of the ``Listed``'s ``Exchange``."""
        return self.exchange.domicile

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.isin

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.isin + "." + self.ticker

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
        create=True,
        **kwargs,
    ):
        """Manufacture/retrieve an instance from the given parameters.

        If a record of the specified class instance does not exist then add it,
        else do nothing. Then return the instance.

        If either an ISIN number or a MIC and Ticker are provided the factory
        shall first attempt retrieval. Failing that if the other parameters are
        sufficient then a new instance shall be committed to the session.

        Note
        ----
        An instances may be retrieved by either an ``isin`` argument or by the
        pair of ``mic`` and ``ticker`` arguments. If none of these is provided
        then a ``ValueError`` exception shall be raised.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        isin : str, Optional
            An International Securities Identification Number (ISIN) uniquely
            identifies a security.
        mic : str, Optional
            ISO 10383 MIC (Market Identifier Code) of the exchange.
        ticker : str, Optional
            The ticker assigned to the asset by the exchange listing process.
        listed_name : str, Optional
            Entity full name of the listed security as it was issued.
        issuer_domicile_code : str(2), Optional
            ISO 3166-1 Alpha-2 two letter country code. The domicile code of the
            issuer.
        issuer_name : str, Optional
            The name of the issuer institution that issued the share.
        status : str
            Flag of listing status ('listed', 'delisted').
        create : bool, Optional
            If `False` then the factory shall expect the specified `Entity` to
            exist in the session or it shall raise an exception.

        The listed security's domicile (and by implication the related currency)
        must already exist in the session or an exception shall be raised.

        The listed security's exchange must already exist in the session or an
        exception shall be raised.

        Note
        ----
        The exchange domicile is considered to be the domicile of the listed
        share. If the parameters don't reflect that an exception shall be
        raised.

        To add a new listing to the session all the parameters except ``mic``
        are required. However if ``mic`` is specified then  ``exchange_name``
        and ``exchange_domicile_code`` are not required.

        To only retrieve a listing from the session one the following
        combinations are required to be specified:

            * The ``isin`` parameter alone.
            * The ``ticker`` and ``mic``.

        Return
        ------
        .Listed
            The single instance that is in the session.

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
                        cls._exchange_id == Exchange.id
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
            obj = cls(listed_name, issuer, isin, exchange, ticker, **kwargs)
            session.add(obj)
        else:
            # Reconcile any changes
            if listed_name and listed_name != obj.name:
                obj.name = listed_name
            if mic and mic != obj.exchange.mic:
                obj.exchange = Exchange.factory(session, mic=mic)
            if ticker and ticker != obj.ticker:
                obj.ticker = ticker
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
    def update_all(cls, session, get_meta_method, get_eod_method=None, **kwargs):
        """Update/create all the objects in the asset_base session.

        Note
        ----
        ``ListedEOD`` may mean a polymorph or child class such as ``ListedEOD``.

        This method updates its class collection of ``ListedEOD`` instances from
        the ``financial_data`` module.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_meta_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all the `factory` method arguments. This is for the
            securities meta-data form which ``Listed`` instances shall be
            created.
        get_eod_method : financial_data module class method, optional
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all the ``ListedEOD.factory`` method arguments. This is
            for the securities time series trade end of day data form which the
            ``ListedEOD`` instances shall be created. If this argument is omitted
            then the ``ListedEOD`` will not be created.

        No object shall be destroyed, only updated, or missing object created.

        """
        # Get securities
        super().update_all(session, get_meta_method, **kwargs)

        # Get EOD trade data.
        if get_eod_method is not None:
            ListedEOD.update_all(session, get_eod_method)

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
        # A table item for  all instances of this class
        dump_dict = dict()
        dump_dict[cls._class_name] = cls.to_data_frame(session)
        # Serialize
        dumper.write(dump_dict)

        # For all class instances in the database get a table for their
        # time-series
        ListedEOD.dump(session, dumper, Listed)

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
        class_name = cls._class_name
        # A table item for  all instances of this class
        # Uses dict data structures. See the docs.
        data_frame_dict = dumper.read(name_list=[class_name])
        cls.from_data_frame(session, data_frame_dict[class_name])

        # For all class instances in the database get a table for their
        # time-series
        ListedEOD.reuse(session, dumper, Listed)

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


class ListedEquity(Listed):
    # TODO:160 Document behaviour. Esp. reg. name and isin changes.
    # TODO:260 Calculate time-series based on holdings. Specify depth to look
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

    id = Column(Integer, ForeignKey("listed.id"), primary_key=True)
    """ Primary key."""

    # Historical Dividend end-of-day (EOD) time-series collection
    # TODO: Rename to dividends
    _dividend_series = relationship(
        "Dividend", order_by=TimeSeriesBase.date_stamp, back_populates="listed_equity"
    )

    # Industry classification
    industry_class = Column(String(16), nullable=True)
    """str: The `industry classification`_ mnemonic in lowercase.

    .. _`industry classification`:
        https://en.wikipedia.org/wiki/Industry_classification

    """
    # Industry classification foreign keys. This is backref'ed as
    # industry_class_icb
    _industry_class_icb_id = Column(
        Integer, ForeignKey("industry_class_icb.id"), nullable=True
    )

    #  A short class name for use in naming
    _name_appendix = "Equity"

    # FIXME: The __repr__ string is printing Currency.__str__ instead of
    # Currency.__repr__

    def __init__(self, name, issuer, isin, exchange, ticker, **kwargs):
        """Instance initialization."""
        super().__init__(name, issuer, isin, exchange, ticker, **kwargs)

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

    @property
    def industry_class_instance(self):
        # TODO: Future integration
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
    def update_all(
        cls,
        session,
        get_meta_method,
        get_eod_method=None,
        get_dividends_method=None,
        **kwargs,
    ):
        """Update/create all the objects in the asset_base session.

        This method updates its class collection of ``ListedEOD`` and
        ``Dividend`` instances from the ``financial_data`` module.

        This method sets the ``Listed.time_series_last_date`` attribute to
        ``datetime.datetime.today()`` for its collection of  ``ListedEOD`` and
        ``Dividend`` instances. This is conditional to the ``last_update``
        argument.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_meta_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with the data items
            in columns named according to this class' ``factory`` method. This
            is for the securities meta-data form which ``Listed`` instances
            shall be created.
        get_eod_method : financial_data module class method, optional
            The method that returns a ``pandas.DataFrame`` with the data items
            in columns named according to the ``ListedEOD`` ``factory`` method.
            This is for the securities time series trade end of day data form
            which the ``ListedEOD`` instances shall be created. If this argument
            is omitted then the ``ListedEOD`` will not be created.
        get_dividend_method : financial_data module class method, optional
            The method that returns a ``pandas.DataFrame`` with the data items
            in columns named according to the ``Dividend`` ``factory``
            method. This is for the securities time series dividend end of day
            data form which the ``Dividend`` instances shall be created.
            If this argument is omitted then the ``Dividend`` will not be
            created.

        No object shall be destroyed, only updated, or missing object created.

        """
        # TODO: Make more intelligent behaviour for new securities fetching.
        # This can be done at the level of the common module where action for
        # new and unseen securities can be taken.

        # Get securities
        super().update_all(session, get_meta_method, get_eod_method, **kwargs)

        # Get Dividend trade data.
        if get_dividends_method is not None:
            Dividend.update_all(session, get_dividends_method)

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
        # Parent class dumper
        super().dump(session, dumper)

        # For all class instances in the database get a table for their
        # time-series
        Dividend.dump(session, dumper, ListedEquity)

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

        # For all class instances in the database get a table for their
        # time-series
        Dividend.reuse(session, dumper, ListedEquity)

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
        dividend_dict_list = [s.to_dict() for s in self._dividend_series]
        if len(dividend_dict_list) == 0:
            raise DividendSeriesNoData(f"Expected dividend data for {self}")
        series = pd.DataFrame(dividend_dict_list)
        series["date_stamp"] = pd.to_datetime(series["date_stamp"])
        series.set_index("date_stamp", inplace=True)
        series.sort_index(inplace=True)  # Assure ascending
        series.name = self

        return series

    def _get_last_dividend(self):
        """Return the dividend last date for the listed asset.

        Raises
        ------
        DividendSeriesNoData
            If no time series exists.
        """
        # Note that _dividend_series is ordered by Dividend.last_date
        try:
            last_dividend = self._dividend_series[-1]
        except IndexError:
            raise DividendSeriesNoData(f"Expected dividend data for {self}")

        return last_dividend

    def get_last_dividend(self):
        """Return the dividend last date for the listed asset.

        Returns
        -------
        dict
            A Dividend price data dictionary with keys from the
            ``time_series.Dividend.to_dict()`` method.

        Raises
        ------
        DividendSeriesNoData
            If no time series exists.
        """
        return self._get_last_dividend().to_dict()

    def get_last_dividend_date(self):
        """Return the dividend last date for the listed asset.

        Returns
        -------
        datetime.date or None
            Last date for the ``.time_series.TimeSeriesBase`` (or child class)
            time series. Returns `None` if no data series exists.
        """
        try:
            last_date = self._get_last_dividend().date_stamp
        except DividendSeriesNoData:
            last_date = None

        return last_date

    def time_series(
        self,
        series="price",
        price_item="close",
        return_type="price",
        identifier="asset",
    ):
        """Retrieve historic time-series for this instance.

        TODO: Remove `series` argument and use to get price series only

        Parameters
        ----------
        series : str
            Which security series:

            'price':
                The security's periodic trade price.
            'dividend':
                The annualized dividend yield.
            'volume':
                The volume of trade (total units of trade) in the period.
        price_item : str
            The specific item of price such as 'close', 'open', `high`, or
            `low`. Only valid when the ``series`` argument is set to 'price'.
        return_type : str
            The specific view of the price series:

            'price':
                The original price series.
            'return':
                The price period-on-period return series.
            'total_return':
                The period-on-period return series inclusive of the extra
                yield due to dividends paid.
            'total_price':
                The price period-on-period price series inclusive of the extra
                yield due to dividends paid. The total_price series start value
                is the same as the price start value.
        identifier : str, optional
            By default the column labels of the returned ``pandas.DataFrame``
            are ``asset.Asset`` (or polymorph child instances) provided by in
            the ``asset_list`` argument. With the `identifier` argument one can
            specify if these column labels are to be substituted:

            'asset':
                The default ``asset.Asset`` (or polymorph child instances)
                provided by in the `asset_list` argument.
            'id':
                The database table `id` column entry.
            'isin':
                The standard security ISO 6166 ISIN number.
            'ticker':
                The exchange ticker
            'identify_code':
                That which will be returned by the
                ``asset.ListedEquity.identity_code`` attribute.
        tidy : bool
            When ``True`` then prices are tidied up by removing outliers.

        Note
        ----
        The data is re-sampled at the daily frequency (365 days per year). Note
        that this may introduce some serial correlations (autocorrelations) into
        the data due to the forward filling of any missing data (NaNs).

        See also
        --------
        Cash.time_series

        """

        def get_prices(price_item):
            eod = self.get_eod()
            try:
                price_series = eod[price_item]
            except KeyError:
                raise ValueError("Unexpected `price_item` argument {price_item}.")
            return price_series

        def get_volumes():
            eod = self.get_eod()
            volume_series = eod["volume"]
            return volume_series

        def get_dividends():
            dividends = self.get_dividend_series()
            dividend_series = dividends["unadjusted_value"]
            return dividend_series

        def get_total_returns(price_item):
            price = get_prices(price_item)
            price_shift = price.shift(1)
            # Try to get dividends if any
            try:
                dividend = get_dividends()
            except DividendSeriesNoData:
                if self.distributions is True:
                    raise DividendSeriesNoData(f"Expected dividend data for {self}.")
                # No dividends
                numerator = price
            else:
                # Warn if not supposed to have dividends
                if self.distributions is False:
                    logger.warning(
                        f"Unexpected dividend data for {self}."
                        "Adding dividends anyway."
                    )
                # If dividends then add them to the price
                numerator = price.add(dividend, fill_value=0.0)
            # Total one period returns
            total_returns = numerator / price_shift
            # First return will be NaN. Default to unity return.
            total_returns.iloc[0] = 1.0
            return total_returns, price

        # The data tidy up method.
        def remove_outliers(price):
            """Tidy up the price series."""
            # Get a numpy array of price values.
            values = price.values
            # Check data has minimum padding length for filtfilt to work
            if values.shape[0] < 7:
                return price
            # Index used for interpolation.
            index = np.arange(np.size(values, axis=0))
            # Tidy up outliers y erasing them with NaN. Repeat until none.
            untidy_columns = np.ones_like(price.columns, dtype=bool)
            # While columns may still be untidy.
            k = 0
            while np.any(untidy_columns) and k < 10:
                k += 1
                # Work copy of untidy columns.
                values_untidy = values[:, untidy_columns]
                # Run a matching forward-backward filter over the price for
                # untidy columns.
                sig1 = filtfilt([1, -1], [1], values_untidy, axis=0)
                # Index of z-scores excursions beyond 3-sigma to create a mask
                # of untidy positions.
                z_score = (sig1 - np.nanmean(sig1, axis=0)) / np.nanstd(sig1, axis=0)
                untidy_mask = abs(z_score) > 3  # True at an untidy position.
                # Detect isolated single tidy data points surrounded by untidy
                # points. These data point are considered to be part of a wider
                # outlier set and shall also to be considered untidy. Value 1.5
                # to reject round-off issues.
                s_test = filtfilt([1, -1], [1], untidy_mask, axis=0)
                single = s_test < -1.5
                untidy_mask |= single
                # Erase data at untidy locations.
                values_untidy[untidy_mask] = np.nan
                # Interpolate each column separately as each has different x_in.
                for i, column in enumerate(values_untidy.T):
                    is_nan = untidy_mask[:, i]
                    not_nan = ~untidy_mask[:, i]
                    x_data = index[not_nan]
                    y_data = column[not_nan]
                    x_in = index[is_nan]
                    y_out = np.interp(x_in, x_data, y_data)
                    column[is_nan] = y_out
                # Replace columns that were tidied up.
                values[:, untidy_columns] = values_untidy
                # Identity and mark columns with all rows that were not untidy.
                # These columns need no further work.
                untidy_columns[untidy_columns] = np.any(untidy_mask, axis=0)

            # Construct a new DataFrame with the tidy time-series.
            price = pd.DataFrame(values, index=price.index, columns=price.columns)

            return price

        if series == "price":
            # Get the price view.
            if return_type == "price":
                result = get_prices(price_item)
            elif return_type == "return":
                price = get_prices(price_item)
                returns = price / price.shift(1)
                # Remove leading and any other NaN with no-returns=1.0.
                result = returns.fillna(1.0)
            elif return_type == "total_price":
                # FIXME: What about multiple dividends on the same day?
                total_returns, price = get_total_returns(price_item)
                total_returns.iloc[0] = price.iloc[0]  # Normalise to start price
                result = total_returns.cumprod()
            elif return_type == "total_return":
                total_returns, price = get_total_returns(price_item)
                result = total_returns
            else:
                raise ValueError(
                    f"Unexpected return_type argument value `{return_type}`."
                )
        elif series == "dividend":
            result = get_dividends()
        elif series == "volume":
            # Get the volume series.
            result = get_volumes()
        else:
            raise ValueError(f"Unexpected series argument value `{series}`.")

        # Add the entity (ListedEquity) as the Series name for later use as
        # column a label in concatenation into a DataFrame
        # TODO: Replace with a match statement
        if identifier == "asset":
            result.name = self
        elif identifier == "id":
            result.name = self.id
        elif identifier == "ticker":
            result.name = self.ticker
        elif identifier == "isin":
            result.name = self.isin
        elif identifier == "identity_code":
            result.name = self.identity_code
        else:
            raise ValueError(f"Unexpected `identifier` argument `{identifier}`.")

        return result


class Index(AssetBase):
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

    id = Column(Integer, ForeignKey("asset_base.id"), primary_key=True)

    key_code_name = "ticker"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    # EOD historical time-series collection ranked by date_stamp
    _eod_series = relationship(
        IndexEOD, order_by=IndexEOD.date_stamp, back_populates="index"
    )
    """list: EOD historical time-series collection ranked by date_stamp

    A list of ``time_series.IndexEOD`` instances.
    """

    # Unique index ticker
    ticker = Column(String(12), nullable=False)

    # Indicates the index time series is a total return price series
    total_return = Column(Boolean, nullable=False)

    # If set True then the index's time series data is static, i.e., not updated
    # and so would be ignored when the ``IndexEOD.update_all`` method is called.
    static = Column(Boolean, nullable=False)

    #  A short class name for use in naming
    _name_appendix = "Index"

    def __init__(
        self, name, ticker, currency, total_return=False, static=False, **kwargs
    ):
        """Instance initialization."""
        super().__init__(name, currency, **kwargs)

        self.ticker = ticker
        self.total_return = total_return
        self.static = static

    def __repr__(self):
        """Return the official string output."""
        msg = '{}(name="{}", ticker="{}", currency={!r})'.format(
            self._class_name, self.name, self.ticker, self.currency
        )

        return msg

    @property
    def key_code(self):
        """Return a unique string code for this class instance."""
        return f"{self.ticker}"

    @property
    def identity_code(self):
        """Return a unique string code for this class instance."""
        return f"{self.ticker}"

    @property
    def long_name(self):
        """str: Return the long name string."""
        msg = "{} is an {} priced in {}.".format(
            self.name, self._class_name, self.currency_ticker
        )

        return msg

    @classmethod
    def factory(cls, session, index_name, ticker, currency_code, create=True, **kwargs):
        """Manufacture/retrieve an instance from the given parameters.

        If a record of the specified class instance does not exist then add it,
        else do nothing. Then return the instance.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        index_name : str
            Entity full name. If the instance does not exist in the session then
            this parameter must be provided to create the instance otherwise an
            exception shall be raised.
        ticker : str
            A short mnemonic code (often derived from the name) used to identity
            the index. This may be used in conjunction with the issuer to
            uniquely identity the index in the world.
        currency_code : str(3)
            ISO 4217 3-letter currency codes.

        Return
        ------
        Index
            The single instance that is in the session.

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
    def update_all(cls, session, get_meta_method, get_eod_method=None, **kwargs):
        """Update/create all the objects in the asset_base session.

        This method updates its class collection of ``Index`` instances from
        the ``financial_data`` module.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_meta_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all the `factory` method arguments. This is for the
            securities meta-data form which ``Index`` instances shall be
            created.
        get_eod_method : financial_data module class method, optional
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all the ``Index.factory`` method arguments. This is
            for the securities time series trade end of day data form which the
            ``IndexEOD`` instances shall be created. If this argument is omitted
            then the ``IndexEOD`` will not be created.

        No object shall be destroyed, only updated, or missing object created.

        """
        # Get securities
        super().update_all(session, get_meta_method, **kwargs)

        # Get EOD trade data.
        if get_eod_method is not None:
            IndexEOD.update_all(session, get_eod_method)


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
    date_stamp : datetime.date
        Date stamp of the data.
    listing_date : datetime.date
        Date the instrument was listed on.
    next_declare_date : datetime.date
        Date of declaration of next dividend
    year_end_month : int
        Month number of financial year end.
    shares_in_issue : int
        Number of shares in issue.
    share_split : float
        number of shares slit into.
    status : str
        Flag of listing status. C - current, S - Suspended.
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

    id = Column(Integer, ForeignKey("listed_equity.id"), primary_key=True)
    """ Primary key."""

    # The index, if any, that the ETF attempts to replicate.
    index = Column(Integer, ForeignKey("index.id"), nullable=True)

    # HACK: These are are workarounds for not having data for all the underlying
    # securities for our ETFs.
    _classes = ("money", "bond", "property", "equity", "commodity", "multi")
    _asset_class = Column(Enum(*_classes))
    _locality = Column(String)

    # Published Total Expense Ratio of the fund.
    ter = Column(Float, nullable=True)

    #  A short class name for use in naming
    _name_appendix = "ETF"

    def __init__(self, name, issuer, isin, exchange, ticker, **kwargs):
        """Instance initialization."""
        super().__init__(name, issuer, isin, exchange, ticker, **kwargs)

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

    def time_series(
        self,
        series="price",
        price_item="close",
        return_type="price",
        identifier="asset",
        tidy=False,
        include_index=False,
    ):
        """Retrieve historic time-series for this instance.

        Parameters
        ----------
        series : str
            Which security series:

            'price':
                The security's periodic trade price.
            'dividend':
                The annualized dividend yield.
            'volume':
                The volume of trade (total units of trade) in the period.
        price_item : str
            The specific item of price such as 'close', 'open', `high`, or
            `low`. Only valid when the ``series`` argument is set to 'price'.
        return_type : str
            The specific view of the price series:

            'price':
                The original price series.
            'return':
                The price period-on-period return series.
            'total_return':
                The period-on-period return series inclusive of the extra
                yield due to dividends paid.
            'total_price':
                The price period-on-period price series inclusive of the extra
                yield due to dividends paid. The total_price series start value
                is the same as the price start value.
        identifier : str, optional
            By default the column labels of the returned ``pandas.DataFrame``
            are ``asset.Asset`` (or polymorph child instances) provided by in
            the ``asset_list`` argument. With the `identifier` argument one can
            specify if these column labels are to be substituted:

            'asset':
                The default ``asset.Asset`` (or polymorph child instances)
                provided by in the `asset_list` argument.
            'id':
                The database table `id` column entry.
            'isin':
                The standard security ISO 6166 ISIN number.
            'ticker':
                The exchange ticker
            'identify_code':
                That which will be returned by the
                ``asset.ListedEquity.identity_code`` attribute.
        tidy : bool
            When ``True`` then prices are tidied up by removing outliers.
        include_index : bool
            When ``True`` then the price series is back-filled with the index
            price series to proxy longer price history. The ``series`` argument
            must be set to 'price' or an exception is raised. This is due to the
            fact that indexes that ETFs replicate typically carry only price
            information.

        Note
        ----
        The data is re-sampled at the daily frequency (365 days per year). Note
        that this may introduce some serial correlations (autocorrelations) into
        the data due to the forward filling of any missing data (NaNs).

        See also
        --------
        Cash.time_series,
        ListedEquity.time_series

        """
        data = super().time_series(series, price_item, return_type, identifier)

        # Do we back-fill the the replicated index time-series history
        if not include_index:
            return data

        # Is there a replicated index with which we can back-fill with the
        # replicated index time-series history
        if self.index is None:
            id_code = self.identity_code
            logger.warning(f"The ExchangeTradeFund {id_code} has no Index reference.")
            return data

        # Now we can back-fill the price with the index time-series history to
        # produce longer histories.

        # Check we are using prices series only
        if series == "price":
            pass
        else:
            raise Exception(
                f"Can back-fill only `price` series, not `{series}` series."
            )

        # Check that we are using like with like, i.e., price and total
        # return price are different indices.
        if return_type in ["price", "return"]:
            if not self.index.total_return:
                raise Exception(
                    "Total price index series cannot back-fill a price series."
                )
        elif return_type in ["total_price", "total_return"]:
            if self.index.total_return:
                raise Exception(
                    "Price index series cannot back-fill a total price series."
                )

        # Get the replicated index for its time-series history as a back-fill
        back_fill = self.index.time_series(series, price_item, return_type, tidy)

        #  Very important that `data` is 1st and `back_fill` is 2nd so that
        #  `back_fill` does nto overwrite any elements in `data`.
        data = data.combine_first(back_fill)

        return data
