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
from sqlalchemy import UniqueConstraint

from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.ext.declarative import declarative_base

from asset_base.exceptions import FactoryError, TimeSeriesNoData, ReconcileError
from asset_base.exceptions import BadISIN

from asset_base.common import Base, Common
from asset_base.entity import Currency, Exchange, Issuer

# Get module-named logger.
import logging

from asset_base.time_series import TradeEOD
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
            msg = '<{}(name="{}", currency="{!r}", owner={!r})>'.format(
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
        entitybase.Cash
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
        """Update/create all the objects in the entitybase session.

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

    def __init__(self, name, issuer, **kwargs):
        """Instance initialization."""
        #  The currency is the issuer's domicile's currency
        currency = issuer.domicile.currency
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
            self.issuer.name, self.domicile.country_name)

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
    # The date of the last update of all time-series instances related to this
    # class. Must be maintained by update logic.
    _eod_series_last_date = Column(Date)

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

        super().__init__(name, issuer, **kwargs)

        # Instrument identification and listing
        self.exchange = exchange
        self.ticker = ticker

        # Currency is the exchange listing currency, i.e., the exchange's
        # domicile currency which overwrites the parent class Share issuer's
        # domicile's currency
        self.currency = exchange.domicile.currency

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
            'issuer_domicile_code': self.domicile.country_code,
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
        finally:
            session.flush()

        return obj

    @classmethod
    def update_all(
            cls, session, get_meta_method, get_eod_method=None,
            **kwargs):
        """ Update/create all the objects in the entitybase session.

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
            TradeEOD.update_all(session, get_eod_method)

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
                'No EOD trade data for  %s' % self.identity_code)
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
