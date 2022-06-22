#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Define classes describing assets such as financial assets.
"""

# TODO: Decide upon key_code and identity_code formats

import sys
import datetime
import numpy as np
import pandas as pd

import stdnum.isin as stdisin

from numpy import abs
from scipy.signal import filtfilt


from sqlalchemy import Float, Integer, String, Enum, Date, Boolean
from sqlalchemy import MetaData, Column, ForeignKey

from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound

from asset_base.exceptions import FactoryError, TimeSeriesNoData, ReconcileError
from asset_base.exceptions import BadISIN

from asset_base.financial_data import Dump

from asset_base.common import Common
from asset_base.entity import Currency, Exchange, Issuer
from asset_base.industry_class import IndustryClassICB
from asset_base.time_series import Dividend, TradeEOD

# Get module-named logger.
import logging


logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Pull in the meta data
metadata = MetaData()


class Asset(Common):
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
    currency : .Currency
        Currency of asset pricing.
    owner : .entity.Entity
        Share owner entity.

    Warning
    -------
    An instance of this class may be an index of a basket of underlying
    invest-able assets. Then it is not a data-like indice of the ``Indice``
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

    __tablename__ = 'asset'
    __mapper_args__ = {'polymorphic_identity': __tablename__, }

    id = Column(Integer, ForeignKey('common.id'), primary_key=True)
    """ Primary key."""

    # Asset currency. Optional.
    _currency_id = Column(Integer, ForeignKey('currency.id'), nullable=True)
    currency = relationship(Currency)

    # Entity owns Asset. Entity has a reference list to many owned Asset named
    # `asset_list`
    # FIXME: Currently owner is allowed to be NULL. Make owner compulsory.
    _owner_id = Column(Integer, ForeignKey('entity.id'))
    owner = relationship(
        'Entity', backref='asset_list', foreign_keys=[_owner_id])

    # TODO: This where we would add asset prices
    # TODO: This is were we would add asset fundamental data relationships
    # TODO: This is were we would add asset book relationships

    # Major asset class. This is a generic class so the asset class is
    # indeterminate.
    _asset_class = None

    def __init__(self, name, currency, **kwargs):
        """Instance initialization."""
        super().__init__(name, **kwargs)

        # Pricing currency
        self.currency = currency

        # Asset owner
        if 'owner' in kwargs:
            self.owner = kwargs.pop('owner')

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        msg = '{} is an {} priced in {}.'.format(
            self.name, self._class_name, self.currency_ticker)
        if self.owner is not None:
            msg += ' Owner: {}'.format(self.owner)

        return msg

    def __repr__(self):
        """Return the official string output."""
        if self.owner is None:
            msg = '<{}(name="{}", currency="{}")>'.format(
                self._class_name, self.name, self.currency)
        else:
            msg = '<{}(name="{}", currency={!r}, owner={!r})>'.format(
                self._class_name, self.name, self.currency, self.owner)

        return msg

    @property
    def domicile(self):
        """.entity.Domicile : ``Domicile`` of the ``Share`` owner ``Entity``."""
        # FIXME: Currently owner is allowed to be NULL. Make owner compulsory.
        if self.owner is None:
            return None
        else:
            return self.owner.domicile

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.currency.ticker + '.' + self.name

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.currency.ticker + '.' + self.name

    @property
    def currency_ticker(self):
        """ISO 4217 3-letter currency code."""
        return self.currency.ticker

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
            obj = session.query(cls).join(Currency).filter(
                cls.name == asset_name,
                Currency.ticker == currency_code
            ).one()
        except NoResultFound:
            if not create:
                raise FactoryError(
                    'Asset "{}", currency="{}", not found.'.format(
                        asset_name, currency_code))
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

    __tablename__ = 'cash'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    id = Column(Integer, ForeignKey('asset.id'), primary_key=True)

    key_code_name = 'asset_currency'
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    _asset_class = 'cash'

    #  A short class name for use in the alt_name method.
    _name_appendix = 'Cash'

    def __init__(self, currency, **kwargs):
        """Instance initialization."""

        assert 'owner' not in kwargs, 'Unexpected `owner` argument.'

        # The name is constrained to that of the currency.
        name = currency.name
        super().__init__(name, currency, **kwargs)

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        msg = '{} is an {} priced in {}.'.format(
            self.name, self._class_name, self.currency_ticker)

        return msg

    def __repr__(self):
        """Return the official string output."""
        msg = '<{}(currency={!r})>'.format(
            self._class_name, self.currency)

        return msg

    @property
    def ticker(self):
        """Currency ticker."""
        return self.currency.ticker

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.ticker

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.ticker

    @classmethod
    def factory(cls, session, ticker, create=True, **kwargs):
        """Manufacture/retrieve an instance from the given parameters.

        Note
        ----
        An existing domicile instead of currency is uses to create a ``Cash``
        instance.

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
                raise FactoryError(f'Currency with ticker `{ticker}` not found.')
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
                'No Currency instances found. '
                'Please run `Currency.update_all`.')
        for currency in currency_list:
            Cash.factory(session, currency.ticker)

    def get_last_eod_trades(self):
        """Return the last EOD price data set in the history

        Returns
        -------
        dict
            A cash price End-Of-Day (EOD) data dictionary with keys :
                date_stamp: The EOD date-time stamp of the data
                price: Same as the closing price for EOD data
                close: The market closing price
            The `close` is the same as the `price`.

        Note
        ----
        The price of ``Cash`` instances will always be 1.0

        """

        # The convention used by this module is to use yesterday's close price
        # due to the limitation imposed by the database price feed.
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        # Round off the date to remove the time and keep only the date
        # component.
        date_stamp = datetime.date(
            yesterday.year, yesterday.month, yesterday.day)
        # The convention is that cash has a price of 1.0.
        price = 1.0

        return {
            "date_stamp": date_stamp,
            "close": price,
        }

    def time_series(self, date_index, *args):
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
        args : tuple of any number of positional arguments, optional
            Dummy arguments to be compatible with general calls to all security
            class ``time_series`` methods. These arguments will be ignored.

        """
        if not isinstance(date_index, pd.DatetimeIndex):
            raise ValueError('Unexpected date_index argument type.')

        # Make a series with all prices set to 1.0
        series = pd.Series(len(date_index) * [1.0], index=date_index)

        # Add the entity (Cash) as the Series name for later use as column a
        # label in concatenation into a DataFrame
        series.name = self

        return series


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

    __tablename__ = 'share'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    id = Column(Integer, ForeignKey('asset.id'), primary_key=True)
    """ Primary key."""

    # TODO: Here we would add share unitization and ownership relationships

    # Issuer issues Share. Issuer has a reference list to many issued Share
    # named `share_list`
    _issuer_id = Column(Integer, ForeignKey('issuer.id'), nullable=False)
    issuer = relationship('Issuer', backref='share_list')

    # Number of share units issued byu the Issuer
    shares_in_issue = Column(Integer, nullable=True)

    # Share price quote in cents or units. Use to always convert to currency
    # units in case of being in cents.
    quote_units = Column(Enum('units', 'cents'), nullable=False)

    #  A short class name for use in the alt_name method.
    _name_appendix = 'Share'

    def __init__(self, name, issuer, currency=None, **kwargs):
        """Instance initialization."""
        # If the currency is not provided then the currency is the issuer's
        # domicile's currency
        if currency is None:
            domicile = issuer.domicile
            currency = domicile.currency

        super().__init__(name, currency, **kwargs)

        self.issuer = issuer

        if 'quote_units' in kwargs:
            self.quote_units = kwargs.pop('quote_units')
        else:
            self.quote_units = 'units'

        # Number of shares issued by the Issuer
        if 'shares_in_issue' in kwargs:
            self.shares_in_issue = kwargs.pop('shares_in_issue')
        else:
            self.shares_in_issue = None

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return '{} is a {} issued by {} in {}.'.format(
            self.name, self._class_name,
            self.issuer.name, self.issuer.domicile.country_name)

    @property
    def domicile(self):
        """.entity.Domicile : ``Domicile`` of the ``Share`` ``Issuer``."""
        return self.domicile

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.issuer.key_code + '.' + self.name

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.issuer.identity_code + '.' + self.name

    def get_locality(self, domicile_code):
        """Return the locality "domestic" or "foreign".

        The "domestic" or "foreign" status of an asset in a current account is
        determined as:

        'domestic'
            If the domicile code of the current account and the asset agree. An
            example would be a South African custody account holding an domestic
            asset of South African domicile.
        'foreign'
            If the domicile code of the current account and the asset disagree.
            An example would be a South African custody account holding an
            foreign asset of German domicile.

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
        share_domicile_code = self.domicile.country_code
        if domicile_code == share_domicile_code:
            locality = 'domestic'
        else:
            locality = 'foreign'

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
        (the National Securities Identifying Number, or NSIN, which identifies
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

    __tablename__ = 'listed'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    id = Column(Integer, ForeignKey('share.id'), primary_key=True)
    """ Primary key."""

    # Exchange lists Listed. Exchange has a reference list to many issued Listed
    # named `securities_list`
    _exchange_id = Column(Integer, ForeignKey('exchange.id'), nullable=False)
    exchange = relationship('Exchange', backref='securities_list')

    # Ticker on the listing exchange.
    ticker = Column(String(12), nullable=False)
    # National Securities Identifying Number
    isin = Column(String(12), nullable=False)

    key_code_name = 'isin'
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    # Historical TradeEOD end-of-day (EOD) time-series collection
    _eod_series = relationship('TradeEOD', backref='listed')

    # Listing status.
    status = Column(Enum('listed', 'delisted'), nullable=False)

    #  A short class name for use in the alt_name method.
    # TODO: Automate from class magic attributes.
    _name_appendix = 'Listed'

    def __init__(self, name, issuer, isin, exchange, ticker, **kwargs):
        """Instance initialization."""

        # Do no remove this code!!. Some methods that use this class (such as
        # factory methods) are able to place arguments with a None value, this
        # circumventing Python's positional-arguments checks. Check manually
        # them here.
        if all([name, issuer, isin, exchange, ticker]):
            pass
        else:
            raise ValueError(
                'Unexpected `None` value for some positional arguments.')

        # Currency is the exchange listing currency, i.e., the exchange's
        # domicile currency which overwrites the parent class Share issuer's
        # domicile's currency
        currency = exchange.domicile.currency

        super().__init__(name, issuer, currency, **kwargs)

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
            raise ValueError(
                'Unexpected domicile. Does not match ISIN country code.')

        # Listing status
        if 'status' in kwargs:
            self.status = kwargs.pop('status')
        else:
            self.status = 'listed'

        # Set last time series update to distant past
        self.time_series_last_date = datetime.date(1900, 1, 1)

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return (
            '{} ({}.{}) ISIN:{} is a {} on the {} issued by {} in {}').format(
                self.name, self.ticker, self.exchange.mic, self.isin,
                self._discriminator, self.exchange.name, self.issuer.name,
                self.domicile.country_name)

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
        return self.isin + '.' + self.ticker

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
        if self.exchange.domicile.country_code == domicile_code:
            locality = 'domestic'
        else:
            locality = 'foreign'

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
            'isin': self.isin,
            'mic': self.exchange.mic,
            'ticker': self.ticker,
            'listed_name': self.name,
            'issuer_name': self.issuer.name,
            'issuer_domicile_code': self.issuer.domicile.country_code,
            'status': self.status,
        }

    @classmethod
    def factory(
            cls, session,
            isin=None, mic=None, ticker=None, listed_name=None,
            issuer_domicile_code=None, issuer_name=None,
            create=True, **kwargs):
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
        issuer_name : str, Optional
            The name of the issuer institution that issued the share.
        issuer_domicile_code : str(2), Optional
            ISO 3166-1 Alpha-2 two letter country code. The domicile code of the
            issuer.
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
                obj = session.query(cls).filter(
                    # Must use explicit join in this line!
                    cls._exchange_id == Exchange.id).filter(
                        Exchange.mic == mic, cls.ticker == ticker).one()
            else:
                raise FactoryError(
                    'Expected either `isin` or `ticker` and `mic` arguments.')
        except NoResultFound:
            # Create and add a new instance below if allowed
            if not create:
                raise FactoryError(
                    'Listed ISIN={}, not found.'.format(isin))
            # Need sufficient arguments. Due to argument default these can be
            # None
            if not all([listed_name, isin, ticker]):
                raise FactoryError(
                    'Expected valid listed_name, isin and ticker arguments. '
                    'Some are None.')
            if not all([issuer_name, issuer_domicile_code]):
                raise FactoryError(
                    'Expected valid issuer_name, issuer_domicile_code '
                    'arguments. Some are None.',
                    action='Creation failed')
            if mic is None:
                raise FactoryError(
                    'Expect valid exchange MIC argument. Got None.')
            # Begin Listed creation process
            try:
                exchange = Exchange.factory(session, mic=mic)
            except FactoryError:
                # The exchange must already exist.
                raise FactoryError(f'Exchange {mic} not found.')
            try:
                issuer = Issuer.factory(
                    session, issuer_name, issuer_domicile_code)
            except FactoryError:
                raise FactoryError(
                    'Could not create or retrieve the Issuer')
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
                raise ReconcileError(obj, 'issuer_name')
            if issuer_domicile_code and \
                    obj.issuer.domicile.country_code != issuer_domicile_code:
                raise ReconcileError(obj, 'issuer_domicile_code')

        return obj

    @classmethod
    def update_all(
            cls, session, get_meta_method, get_eod_method=None,
            **kwargs):
        """ Update/create all the objects in the asset_base session.

        This method updates its class collection of ``TradeEOD`` instances from
        the ``financial_data`` module.

        This method sets the ``Listed.time_series_last_date`` attribute to
        ``datetime.datetime.today()`` for its collection of  ``TradeEOD``
        instances. This is conditional to the ``last_update`` argument.

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
            same name as all the ``TradeEOD.factory`` method arguments. This is
            for the securities time series trade end of day data form which the
            ``TradeEOD`` instances shall be created. If this argument is omitted
            then the ``TradeEOD`` will not be created.

        No object shall be destroyed, only updated, or missing object created.

        """
        # Get securities
        super().update_all(session, get_meta_method, **kwargs)

        # Get EOD trade data.
        if get_eod_method is not None:
            TradeEOD.update_all(session, cls, get_eod_method)

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
        TradeEOD.dump(session, dumper, Listed)

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
        TradeEOD.reuse(session, dumper, Listed)

    def get_eod_trade_series(self):
        """Return the EOD trade data series for the security.

        Returns
        -------
        pandas.DataFrame
            An EOD trade data time series with a ``pandas.Timestamp`` date index
            sorted in ascending order and columns: `adjusted_close`, `close`,
            `high`, `low`, `open`, `price` and `volume`. The `price` column is
            identical to the `close` column. The ``pandas.DataFrame.name`` shall
            be the ``Listed`` security instance.

        """
        trade_eod_dict_list = [s.to_dict() for s in self._eod_series]
        if len(trade_eod_dict_list) == 0:
            raise TimeSeriesNoData(
                f'No EOD trade data for  security {self.identity_code}.')
        series = pd.DataFrame(trade_eod_dict_list)
        series['date_stamp'] = pd.to_datetime(series['date_stamp'])
        series.set_index('date_stamp', inplace=True)
        series.sort_index(inplace=True)  # Assure ascending
        series.name = self
        return series

    def get_last_eod_trades(self):
        """Return the last EOD trade data for the security.

        Returns
        -------
        dict
            An End-Of-Day (EOD) price data dictionary with keys from the
            ``TradeEOD.to_dict()`` method.
        """
        # TODO: There must be a more efficient algo to fetch the last price set.
        last_eod_dict = self.get_eod_trade_series().iloc[-1].to_dict()

        # Return the dictionary of price items
        return last_eod_dict

    def get_live_trades(self):
        # TODO: The required methods are not currently functioning.
        """Return the live trade data if available else use the last EOD trades.

        This method shall first try to get live data from a feed and if that
        fails it shall return the last EOD price.


        Returns
        -------
        dict
            If live prices are available then the live price data dictionary
            will be returned from the class ``get_live_trades`` method, else if
            live prices are not available then the End-Of-Day (EOD) price data
            dictionary will be returned from the class ``get_last_eod_trades``
            method.

        See also
        --------
        .get_live_trades
        .get_last_eod_trades
        """
        try:
            data = self.get_live_trades()  # FIXME:
        except NoResultFound:
            data = self.get_last_eod_trades()

        # Fetch the date filtered range of time-series data.
        return data

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
        (the National Securities Identifying Number, or NSIN, which identifies
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
    exchange_board : str
        Board of the exchange that the instrument is listed on.
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

    __tablename__ = 'listed_equity'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    # Major asset class constant. Possibly be overridden by child classes.
    _asset_class = 'equity'

    id = Column(Integer, ForeignKey('listed.id'), primary_key=True)
    """ Primary key."""

    # Historical Dividend end-of-day (EOD) time-series collection
    _dividend_series = relationship('Dividend', backref='listed_equity')

    # Industry classification
    industry_class = Column(String(16), nullable=True)
    """str: The `industry classification`_ mnemonic in lowercase.

    .. _`industry classification`:
        https://en.wikipedia.org/wiki/Industry_classification

    """
    # Industry classification foreign keys. This is backref'ed as
    # industry_class_icb
    _industry_class_icb_id = Column(Integer,
                                    ForeignKey('industry_class_icb.id'),
                                    nullable=True)

    #  A short class name for use in the alt_name method.
    _name_appendix = 'Equity'

    def __init__(self, name, issuer, isin, exchange, ticker, **kwargs):
        """Instance initialization."""
        super().__init__(
            name, issuer, isin, exchange, ticker, **kwargs)

        # Select industry classification scheme, initialise and add it.
        if 'industry_class' in kwargs:
            if kwargs['industry_class'] == 'icb':
                self.industry_class = kwargs.pop('industry_class')
                # Create and assign the industry classification instance
                self._industry_class_icb = IndustryClassICB(
                    industry_name=kwargs.pop('industry_name'),
                    super_sector_name=kwargs.pop('super_sector_name'),
                    sector_name=kwargs.pop('sector_name'),
                    sub_sector_name=kwargs.pop('sub_sector_name'),
                    industry_code=kwargs.pop('industry_code'),
                    super_sector_code=kwargs.pop('super_sector_code'),
                    sector_code=kwargs.pop('sector_code'),
                    sub_sector_code=kwargs.pop('sub_sector_code'),
                )
            else:
                raise ValueError(
                    'The `industry_class` {} is not implemented.'.format(
                        self.industry_class))

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
        if self.industry_class == 'icb':
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
            'industry_class': self.industry_class,
            'industry_name': self._industry_class_icb.industry_name,
            'super_sector_name': self._industry_class_icb.super_sector_name,
            'sector_name': self._industry_class_icb.sector_name,
            'sub_sector_name': self._industry_class_icb.sub_sector_name,
            'industry_code': self._industry_class_icb.industry_code,
            'super_sector_code': self._industry_class_icb.super_sector_code,
            'sector_code': self._industry_class_icb.sector_code,
            'sub_sector_code': self._industry_class_icb.sub_sector_code,
        }
        dictionary.update(additional_dict)

        return dictionary

    @classmethod
    def update_all(
            cls, session, get_meta_method,
            get_eod_method=None, get_dividends_method=None,
            **kwargs):
        """ Update/create all the objects in the asset_base session.

        This method updates its class collection of ``TradeEOD`` and
        ``Dividend`` instances from the ``financial_data`` module.

        This method sets the ``Listed.time_series_last_date`` attribute to
        ``datetime.datetime.today()`` for its collection of  ``TradeEOD`` and
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
            in columns named according to the ``TradeEOD`` ``factory`` method.
            This is for the securities time series trade end of day data form
            which the ``TradeEOD`` instances shall be created. If this argument
            is omitted then the ``TradeEOD`` will not be created.
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
        super().update_all(
            session, get_meta_method, get_eod_method, **kwargs)

        # Get Dividend trade data.
        if get_dividends_method is not None:
            Dividend.update_all(session, cls, get_dividends_method)

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
        Dividend.reuse(session, dumper, Listed)

    def get_dividend_series(self):
        """Return the dividends data series for the security.
        """
        dividend_dict_list = [s.to_dict() for s in self._dividend_series]
        if len(dividend_dict_list) == 0:
            raise TimeSeriesNoData(
                'No dividend data for  %s' % self.identity_code)
        series = pd.DataFrame(dividend_dict_list)
        series['date_stamp'] = pd.to_datetime(series['date_stamp'])
        series.set_index('date_stamp', inplace=True)
        series.sort_index(inplace=True)  # Assure ascending
        series.name = self
        return series

    # FIXME: Maybe supposed to be with Listed (where the _time_series attr is)
    def time_series(self,
                    series='price', price_item='close', return_type='price',
                    tidy=False):
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
                The volume of trade (total value of trade) in the period.
        price_item : str
            The specific item of price. Only valid when the ``series`` argument
            is set to 'price':

            'close' :
                The period's close price.
            'open' :
                The period's open price.
            'low' :
                The period's lowest price.
            'high' :
                The period's highest price.
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
        tidy : bool
            When ``True`` then prices are tidied up by removing outliers.

        Note
        ----
        The data is re-sampled at the daily frequency (365 days per year). Note
        that this may introduce some serial correlations (autocorrelations) into
        the data due to the forward filling of any missing data (NaNs).

        See also
        --------
        .Cash.get_time_series_data_frame, .EntityBase.get_time_series_data_frame

        """
        def get_price_series(price_item):
            eod = self.get_eod_trade_series()
            try:
                price_series = eod[price_item]
            except pd.KeyError:
                raise ValueError('Unexpected `price_item` argument.')
            # Adjust for quotes that are in cents
            if self.quote_units == 'cents':
                price_series /= 100.0
            return price_series

        def get_volume_series():
            eod = self.get_eod_trade_series()
            volume_series = eod['volume']
            return volume_series

        def get_dividend_series():
            dividends = self.get_dividend_series()
            dividend_series = dividends['adjusted_value']
            # Adjust for quotes that are in cents
            if self.quote_units == 'cents':
                dividend_series /= 100.0
            return dividend_series

        def get_total_returns(price_item):
            price = get_price_series(price_item)
            price_shift = price.shift(1)
            # Try to get dividends if any
            try:
                dividends = get_dividend_series()
            except TimeSeriesNoData:
                # New securities may not have dividends yet so warn.
                logger.warning(
                    f'No dividend data for security {self.identity_code}.')
            else:
                total_price = price.add(dividends, fill_value=0.0)
            # Total one period returns
            total_returns = total_price / price_shift
            # First return will NaN. Set to unity return.
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
                z_score = (sig1 - np.nanmean(sig1, axis=0)
                           ) / np.nanstd(sig1, axis=0)
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
            price = pd.DataFrame(values,
                                 index=price.index, columns=price.columns)

            return price

        if series == 'price':
            # Get the price view.
            if return_type == 'price':
                result = get_price_series(price_item)
            elif return_type == 'return':
                price = get_price_series(price_item)
                returns = price / price.shift(1)
                # Remove leading and any other NaN with no-returns=1.0.
                result = returns.fillna(1.0)
            elif return_type == 'total_price':
                # FIXME: What about multiple dividends on the same day?
                total_returns, price = get_total_returns(price_item)
                total_returns.iloc[0] = price.iloc[0]
                result = total_returns.cumprod()
            elif return_type == 'total_return':
                total_returns, price = get_total_returns(price_item)
                result = total_returns
            else:
                raise ValueError(
                    f'Unexpected return_type argument value `{return_type}`.')
        elif series == 'dividend':
            result = get_dividend_series()
        elif series == 'volume':
            # Get the volume series.
            result = get_volume_series()
        else:
            raise ValueError(
                f'Unexpected series argument value `{series}`.')

        # Add the entity (ListedEquity) as the Series name for later use as
        # column a label in concatenation into a DataFrame
        result.name = self

        return result


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
        (the National Securities Identifying Number, or NSIN, which identifies
        the security, padded as necessary with leading zeros), and one numerical
        check digit.
    exchange : .Exchange
        The exchange the asset is listed upon.
    ticker : str
        The ticker assigned to the asset by the exchange listing process.
    exchange_board : str
        Board of the exchange that the instrument is listed on.
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

    __tablename__ = 'exchange_traded_fund'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    id = Column(Integer, ForeignKey('listed_equity.id'), primary_key=True)
    """ Primary key."""

    # HACK: These are are workarounds for not having data for all the underlying
    # securities for our ETFs.
    _classes = ('money', 'bond', 'property', 'equity', 'commodity')
    _asset_class = Column(Enum(*_classes))
    _locality = Column(String)

    # If True then the fund roll up distributions by reinvesting them, i.e.,
    # this is a total return fund.
    roll_up = Column(Boolean)

    # Published Total Expense Ratio of the fund.
    ter = Column(Float)

    #  A short class name for use in the alt_name method.
    _name_appendix = 'ETF'

    def __init__(self, domicile, name, issuer, isin, exchange, ticker,
                 **kwargs):
        """Instance initialization."""
        # Optional parameters.
        if 'asset_class' in kwargs:
            self._asset_class = kwargs['asset_class']
        if 'locality' in kwargs:
            self._locality = kwargs['locality']
        if 'roll_up' in kwargs:
            roll_up = kwargs['roll_up']
            if roll_up in (True, 'TRUE', 'True', 'true'):
                self.roll_up = True
            elif roll_up in (False, 'FALSE', 'False', 'false'):
                self.roll_up = False
            else:
                raise ValueError('Unexpected "roll_up" argument.')
        if 'ter' in kwargs:
            self.ter = kwargs['ter']

        super().__init__(
            domicile, name, issuer, isin, exchange, ticker,
            **kwargs)

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
        'undefined':
            If the domicile code of the current account and only some of the
            fund's underlying securities domicile codes disagree.

        Note
        ----
        Currently the "domestic" or "foreign" locality status is determined by
        external information entered into the asset_base as we don't currently
        carry information regarding the underlying securities.

        """
        if self._locality == domicile_code:
            locality = 'domestic'
        else:
            locality = 'foreign'

        return locality
