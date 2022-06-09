#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Define classes describing entities such as legal and natural persons.
"""
# TODO: Decide upon key_code and identity_code formats

import sys
import base64
import hashlib
import networkx
import datetime

import pandas as pd

from sqlalchemy import Float, Integer, String, Date
from sqlalchemy import MetaData, Column, ForeignKey
from sqlalchemy import UniqueConstraint

from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.ext.declarative import declarative_base

from asset_base.common import Base, Common
from asset_base.exceptions import FactoryError, HoldingsError, ReconcileError


# Get module-named logger.
import logging
logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Pull in the meta data
metadata = MetaData()


class Currency(Base):
    """ISO 4217 world currency descriptions.

    Currency is treated here as a notion and not an asset, therefore it does not
    find itself as a child class of the ``Asset`` class and is therefore not and
    ``Entity``.

    SO 4217 is the International Standard for currency codes. The most recent
    edition is ISO 4217:2008.

    The purpose of ISO 4217:2008 is to establish internationally recognized
    codes for the representation of currencies. Currencies can be represented
    in the code in two ways: a three- letter alphabetic code and a three-digit
    numeric code.

    Alphabetic code

        The alphabetic code is based on another ISO standard, ISO 3166, which
        lists the codes for country names. The first two letters of the ISO
        4217 three-letter code are the same as the code for the country name,
        and where possible the third letter corresponds to the first letter of
        the currency name.

        For example:

            * The US dollar is represented as USD - the US coming from the ISO
            * 3166 country code and the D for dollar.

            * The Swiss franc is represented by CHF - the CH being the code for
            * Switzerland in the ISO 3166 code and F for franc. Numeric code

    Numeric code

        The three-digit numeric code is useful when currency codes need to be
        understood in countries that do not use Latin scripts and for
        computerized systems. Where possible the 3 digit numeric code is the
        same as the numeric country code.

        For currencies having minor units, ISO 4217:2008 also shows the
        relationship between the minor unit and the currency itself (i.e.
        whether it divides into 100 or 1000).

    An `ISO 4217:2008`_ currency file may be downloaded in Excel format.

    .. _`ISO 4217:2008`:
        http://www.currency-iso.org/dam/downloads/lists/list_one.xls


    Parameters
    ----------
    ticker : str(3)
        ISO 4217 3-letter currency codes.
    name : str
        ISO 4217 currency names.

    Note
    ----
    This is a fundamental support class. That means that all the data for this
    class must already exist in the database before any entities are introduced
    into the database.

    Attributes
    ----------
    ticker : str(3)
        ISO 4217 3-letter currency codes.
    name : str
        ISO 4217 currency names.


    See also
    --------
    .Domicile

    """

    __tablename__ = 'currency'
    __table_args__ = (UniqueConstraint('ticker'),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    """ Primary key."""

    # Data.
    ticker = Column(String(3), nullable=False)
    name = Column(String(256), nullable=False)

    def __init__(self, ticker, name):
        """Instance initialization."""
        self.ticker = ticker
        self.name = name

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return '{} is {} ({})'.format(self._class_name, self.name, self.ticker)

    def __repr__(self):
        """Return the official string output."""
        return '<{}(ticker="{}", name="{}")>'.format(
            self._class_name, self.ticker, self.name)

    @classmethod
    @property
    def _class_name(cls):
        return cls.__name__

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.ticker

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.ticker

    @classmethod
    def factory(cls, session, ticker, name=None):
        """Manufacture/retrieve an instance from the given parameters.

        If a record of the specified class instance does not exist then add it,
        else do nothing. Then return the instance.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        ticker : str
            ISO 4217 3-letter currency code.
        name : str
            ISO 4217 currency names. Required if the specified currency instance
            is not already in the session, else an exception shall be raised; as
            the instance still needs to be created. If the instance does exist
            then this name must match that of the instance or an  exception
            shall be raised.

        Return
        ------
        .Currency
            The single instance that is in the session.

        """
        assert len(ticker) == 3, \
            'Expected ISO 4217 3-letter currency code.'

        # Check if currency exists in the session and if not then add it.
        try:
            obj = session.query(cls).filter(cls.ticker == ticker).one()
        except NoResultFound:
            # Create a new instance if possible
            if all([ticker, name]):
                obj = cls(ticker, name)
                session.add(obj)
            else:
                raise FactoryError(
                    'Expected all positional arguments',
                    action='Creation failed')
        else:
            # Reconcile possible changes
            if name and name != obj.name:
                raise FactoryError(
                    'Unexpected name {} for currency {}'.format(
                        name, obj.ticker)
                )

        return obj

    @classmethod
    def from_data_frame(cls, session, data_frame):
        """Create multiple class instances in the session from a dataframe.

        The instances are added to the session.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        data_frame : pandas.DataFrame
            A ``pandas.DataFrame`` with columns of the same name as all the
            `factory` method arguments, with the exception of the `session`
            argument.

        """
        for i, row in data_frame.iterrows():
            cls.factory(session, **row)

    @classmethod
    def update_all(cls, session, get_method):
        """ Update/create all the objects in the entitybase session.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all the `factory` method arguments, with the exception
            of the `session` argument.

        No object shall be destroyed, only updated, or missing object created.

        """
        # Get all financial data
        data = get_method()
        # Bulk add/update data (uses the factory method)
        cls.from_data_frame(session, data)


class Domicile(Base):
    """ISO 3166 domicile country descriptions.

    ISO 3166 is the International Standard for country codes and codes for
    their subdivisions.

    The purpose of ISO 3166 is to define internationally recognized codes of
    letters and/or numbers that we can use when we refer to countries and
    subdivisions. However, it does not define the names of countries - this
    information comes from United Nations sources (Terminology Bulletin Country
    Names and the Country and Region Codes for Statistical Use maintained by
    the United Nations Statistics Divisions).

    Using codes saves time and avoids errors as instead of using a country's
    name (which will change depending on the language being used) we can use a
    combination of letters and/or numbers that are understood all over the
    world.

    For example, all national postal organizations throughout the world
    exchange international mail in containers identified with the relevant
    country code. Internet domain name systems use the codes to define top
    level domain names such as '.fr' for France, '.au' for Australia. In
    addition, in machine readable passports, the codes are used to determine
    the nationality of the user and when we send money from one bank to another
    the country codes are a way to identify where the bank is based.

    An ISO 3166 country file can be `downloaded in CSV format`_.

    .. _`downloaded in CSV format`:
        https://commondatastorage.googleapis.com/ckannet-storage/
        2011-11-25T132653/iso_3166_2_countries.csv

    Attributes
    ----------
    country_code : str(2)
        ISO 3166-1 Alpha-2 two letter country code.
    name : str
        ISO 3166-1 country name.

    Note
    ----
    This is a fundamental support class. That means that all the data for this
    class must already exist in the database before any entities are introduced
    into the database.


    See also
    --------
    .Currency

    Attributes
    ----------
    country_code : str(2)
        ISO 3166-1 Alpha-2 two letter country code.
    name : str
        ISO 3166-1 country name.

    """

    __tablename__ = 'domicile'

    id = Column(Integer, primary_key=True, autoincrement=True)
    """ int : Primary key."""

    # Official currency
    _currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    currency = relationship('Currency')

    # Data.
    country_code = Column(String(3), nullable=False)
    country_name = Column(String(256), nullable=False)

    # The ISO 3166-1 Alpha-2 two letter country code is a unique identifier of a
    # country
    __table_args__ = (UniqueConstraint('country_code'),)

    # Reference to domiciled entities
    # entity_list = relationship('Entity', back_populates='domicile')

    def __init__(self, country_code, country_name, currency):
        """Instance initialization."""
        self.country_code = country_code
        self.country_name = country_name
        self.currency = currency

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return '{} is {} ({})'.format(
            self._class_name, self.country_name, self.country_code)

    def __repr__(self):
        """Return the official string output."""
        return \
            '{}(country_code="{}", country_code="{}", currency="{!r}")'.format(
                self._class_name, self.country_code, self.country_name,
                self.currency)

    @classmethod
    @property
    def _class_name(cls):
        return cls.__name__

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.country_code

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.country_code

    @classmethod
    def factory(cls, session, country_code, country_name=None, currency_ticker=None):
        """Retrieve an existing, or manufacture a missing, class instance.

        If a record of the specified class instance does not exist then add it,
        else do nothing. Return the instance.

        The related `Currency` instance must already exist or a `FactoryError`
        exception shall be thrown.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        country_code : str(2)
            ISO 3166-1 Alpha-2 two letter country code.
        country_name : str
            ISO 3166-1 country name. Required to create a new instance.
        currency_ticker : str
            ISO 4217 3-letter currency codes. Required to create a new instance.

        Return
        ------
        .Domicile
            The single instance that is in the session.

        See also
        --------
        .Currency.factory,

        """
        assert len(country_code) == 2, \
            'Expected ISO 3166-1 Alpha-2 two letter country code.'

        # Check if domicile exists in the session and if not then add it.
        try:
            obj = session.query(cls).filter(cls.country_code == country_code).one()
        except NoResultFound:
            arg_list = [country_code, country_name, currency_ticker]
            if not all(arg_list):
                raise FactoryError(
                    'Domicile Creation Failed. '
                    'Expected all positional arguments.',
                    action='Create failed')
            currency = Currency.factory(session, currency_ticker)
            obj = cls(country_code, country_name, currency)
            session.add(obj)
        else:
            # Disallow changes
            if country_name and country_name != obj.country_name:
                raise FactoryError(
                    'Invalid country_name argument for country '
                    '{}.'.format(obj.country_code))
            if currency_ticker and currency_ticker != obj.currency.ticker:
                raise FactoryError(
                    'Invalid currency_ticker argument for country '
                    '{}.'.format(obj.country_code))

        return obj

    @classmethod
    def from_data_frame(cls, session, data_frame):
        # FIXME: Convert to a mixin
        """Create multiple class instances in the session from a dataframe.

        The instances are added to the session.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        data_frame : pandas.DataFrame
            A ``pandas.DataFrame`` with columns of the same name as all the
            `factory` method arguments, with the exception of the `session`
            argument.

        """
        for i, row in data_frame.iterrows():
            cls.factory(session, **row)

    @classmethod
    def update_all(cls, session, get_method):
        # FIXME: Convert to a mixin
        """ Update/create all the objects in the entitybase session.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all the `factory` method arguments, with the exception
            of the `session` argument.

        No object shall be destroyed, only updated, or missing object created.

        """
        # Get all financial data
        data = get_method()
        # Bulk add/update data (uses the factory method)
        cls.from_data_frame(session, data)


class Entity(Common):
    """The base class for all entities.

    Note
    ----
    This is an abstract class not meant for direct instantiation.


    An entity is something that exists in itself, actually or potentially,
    concretely or abstractly, physically or not. It need not be of material
    existence. In particular, abstractions and legal fictions are usually
    regarded as entities. In general, there is also no presumption that an
    entity is animate - Wikipedia.

    Whilst the unique identifier for this class is it's primary key attribute
    ``id``, this entity's instances are uniquely identified by their name and
    domicile.

    This is a polymorphic class and may take on the identity of any of its child
    classes by means of a properly set up internal discriminator.

    This classes has the capability to render entity holding tree structures
    where entities hold one or more child entities and so on. A weight, or
    rather a weighted graph *edge*, which connects a parent entity (*vertice*)
    to child entity (*vertice*) by the holding weight is represented by an
    `EntityWeight` instance.

    Assume this `Entity` instance holds one or more child `Entity` instances,
    which themselves hold child `Entity` instances. This may be represented by
    this class with an acyclic directed tree graph in which all nodes are
    reachable only by moving from parent to child starting from from the top
    parent node which is the instance calling this method. The resulting holding
    weights of these entities may also be computed.

    In mathematics and computer science, a directed acyclic graph, is a finite
    directed graph with no directed cycles. That is, it consists of finitely
    many vertices and edges, with each edge directed from one vertex to another,
    such that there is no way to start at any vertex v and follow a
    consistently-directed sequence of edges that eventually loops back to v
    again. Equivalently, a DAG is a directed graph that has a topological
    ordering, a sequence of the vertices such that every edge is directed from
    earlier to later in the sequence -- Wikipedia_.

    .. _Wikipedia:
        https://en.wikipedia.org/wiki/Directed_acyclic_graph

    Parameters
    ----------
    name : str
        Entity full name.
    alt_name : str, optional
        An optional, alternative name to the ``name`` parameter. If provided the
        ``name`` attribute shall default to this.
    domicile : .Domicile
        Domicile of the entity.
    children : list
        A list of `EntityWeight` instances. Each item in the list represents
        the holding of a child entity and it's holding weight.

    See also
    --------
    .Currency, .Domicile, .EntityWeight

    Attributes
    ----------
    id : int Unique integer entity id.
    name : str
        Entity full name. I there is an alternative name then it will supersede
        the name.
    domicile : .Domicile
        Domicile of the entity.
    key : str, Optional
        A string created by the class, which, when combined with the
        _discriminator attribute shall constitute a unique constraint. It shall
        be up to the child to implement this string, based on what makes an
        instance of that class unique. The string shall be a 32-character
        maximum. If no argument is provided then the Entity instance name and
        domicile 2-letter ISO country_code shall be used.
    parent_weight : `EntityWeight`
        An `EntityWeight` instance representing a parent entity holding the
        current instance.
    children_weights : list
        An list of `EntityWeight` instances representing the holding of child
        entities by this the parent entity. See the `get_weights` method.

    """

    __tablename__ = 'entity'
    __mapper_args__ = {'polymorphic_identity': __tablename__, }

    id = Column(Integer, ForeignKey('common.id'), primary_key=True)
    """ Primary key."""

    # Entity's domicile. Domicile has a reference list to many domiciled Entity
    # named `entity_list`
    _domicile_id = Column(Integer, ForeignKey('domicile.id'), nullable=False)
    domicile = relationship('Domicile', backref='entity_list')

    def __init__(self, name, domicile, **kwargs):
        """Instance initialization."""
        super().__init__(name, **kwargs)
        self.domicile = domicile

        # Record the current date as the system entry date of the model.
        self.date_create = datetime.date.today()
        self.date_mod_stamp = None  # Never been modified

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return '{} is an {} in {}'.format(
            self.name, self._class_name, self.domicile.country_name)

    def __repr__(self):
        """Return the official string output."""
        return '<{}(name="{}", domicile="{!r}")>'.format(
            self._class_name, self.name, self.domicile)

    @property
    def currency(self):
        """Currency : ``Currency`` of the ``Entity`` ``Domicile``."""
        return self.domicile.currency

    @property
    def alt_name(self):
        if self._alt_name:
            name = self._alt_name + ' ' + self._class_name
        else:
            name = self.name + ' ' + self._class_name

        return name

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.domicile.key_code + '.' + self.name

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.domicile.identity_code + '.' + self.name

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
            'entity_name': self.name,
            'country_code': self.domicile.country_code,
        }

    @classmethod
    def factory(cls, session, entity_name, country_code, create=True,
                **kwargs):
        """Manufacture/retrieve an instance from the given parameters.

        If a record of the specified class instance does not exist then add it,
        else do nothing. Then return the instance.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        entity_name : str
            Entity full name.
        country_code : str(2)
            ISO 3166-1 Alpha-2 two letter country code.
        children : dict, optional
            A dictionary of entity holding weights. The specified child `Entity`
            instances must already exist in the session or an exception shall be
            raised.

            Build the dictionary as follows::

                children = {
                    'entity1.id': weight1,
                    ...,
                    `entityN.id`: weightN,
                }

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
            obj = session.query(cls).join(Domicile).filter(
                cls.name == entity_name,
                Domicile.country_code == country_code
            ).one()
        except NoResultFound:
            if not create:
                raise FactoryError(
                    'Entity "{}", domicile="{}", not found.'.format(
                        entity_name, country_code))
            domicile = Domicile.factory(session, country_code)
            obj = cls(entity_name, domicile, **kwargs)
            session.add(obj)
        else:
            # No changes to reconcile, country_code and entity_name are the key
            # arguments.
            pass

        # TODO: Make this an add_children method instead & create a unittest.
        #  Check for children in keyword arguments.
        if 'children' in kwargs:
            # Find all child Entity instances specified by the children dict.
            children = kwargs.pop('children')
            child_list = list()
            for id, value in children.items():
                # Query (find) and add.
                try:
                    item = session.query(Entity).filter(Entity.id == id).one()
                except NoResultFound:
                    raise FactoryError(
                        'Child Entity, id=%i, not found.' % id)
                else:
                    child_list.append((item, value))
            #  Add the list of children as `EntityWeight` instances.
            obj._add_entity_weights(session, child_list)

        return obj


class Institution(Entity):
    """An institution from a financial perspective.

    Note
    ----
    This is an abstract class not meant for direct instantiation.

    Parameters
    ----------
    name : str
        Entity full name.
    domicile : .Domicile
        Domicile of the entity.

    Note
    ----
    This class should not be directly instantiated.

    See also
    --------
    .Entity

    """

    __tablename__ = 'institution'
    __mapper_args__ = {'polymorphic_identity': __tablename__, }

    id = Column(Integer, ForeignKey('entity.id'), primary_key=True)
    """ Primary key."""

    def __init__(self, name, domicile, **kwargs):
        """Instance initialization."""
        super().__init__(name=name, domicile=domicile, **kwargs)


class Issuer(Institution):
    """An issuer of shares, indices, models, etc.

    Issuer is a legal entity that develops, registers and sells securities
    (shares) for the purpose of financing its operations. Issuers may be
    domestic or foreign governments, corporations or investment trusts. They
    are institutions.

    The most common types of securities issued are common and preferred stocks,
    bonds, notes, debentures and bills.

    See also
    --------
    .Institution,
    """

    __tablename__ = 'issuer'
    __mapper_args__ = {'polymorphic_identity': __tablename__, }

    id = Column(Integer, ForeignKey('institution.id'), primary_key=True)
    """ Primary key."""

    # Collection of the issued Model instances.
    # TODO: Use backref in Model

    def __init__(self, name, domicile, **kwargs):
        """Instance initialization."""
        super().__init__(name=name, domicile=domicile, **kwargs)


class Exchange(Institution):
    """Stock Exchanges identified by ISO 10383 MICs (Market Identifier Codes).

    ISO 10383 is an ISO standard which "specifies a universal method of
    identifying exchanges, trading platforms and regulated or non-regulated
    markets as sources of prices and related information in order to facilitate
    automated processing." The codes defined by the standard are known as
    Market Identifier Codes, or MICs. The FIX Protocol uses MICs to represent
    values of the Fix Exchange data type. Markets of various asset classes,
    including equities, options and futures apply for MICs through the ISO. New
    MICs are added frequently and the latest published list is available at the
    `maintenance organization of ISO 10383`_.

    .. _`maintenance organization of ISO 10383`:
        http://www.iso15022.org/MIC/homepageMIC.htm

    Parameters
    ----------
    name : str
        The name of the exchange.
    domicile : .Domicile
        Domicile of the exchange.
    mic : str
        ISO 10383 MIC (Market Identifier Code) of the exchange.
    eod_code : str, optional
        EODHistoricalData.com exchange code. See
        https://eodhistoricaldata.com/financial-apis/list-supported-exchanges/

    See also
    --------
    .Institution,

    Attributes
    ----------
    mic : str
        ISO 10383 MIC (Market Identifier Code) of the exchange.

    """

    __tablename__ = 'exchange'
    __mapper_args__ = {'polymorphic_identity': __tablename__, }

    id = Column(Integer, ForeignKey('institution.id'), primary_key=True)
    """ Primary key."""

    # List of listed Shares on the Exchange
    # TODO: Use backref in Listed

    # Data.
    mic = Column(String(4), nullable=False)
    eod_code = Column(String(6), nullable=True)

    key_code_name = 'mic'
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    def __init__(self, name, domicile, mic, **kwargs):
        """Instance initialization."""
        self.mic = mic

        if 'eod_code' in kwargs:
            self.eod_code = kwargs.pop('eod_code')

        super().__init__(name, domicile, **kwargs)

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return '{} ({}) is an {} in {}'.format(
            self.name, self.mic, self._class_name, self.domicile.country_name)

    def __repr__(self):
        """Return the official string output."""
        return '<{}(name="{}", domicile="{!r}", mic="{}")>'.format(
            self._class_name, self.name, self.domicile, self.mic)

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.mic

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.mic

    @classmethod
    def factory(
            cls, session, mic, exchange_name=None, country_code=None,
            create=True,
            **kwargs):
        """Manufacture/retrieve an instance from the given parameters.

        If a record of the specified class instance does not exist then add it,
        else do nothing. Then return the instance.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        mic : str
            ISO 10383 MIC (Market Identifier Code) of the exchange.
        exchange_name : str
            Entity full name. If the instance does not exist in the session then
            this parameter must be provided to create the instance otherwise an
            exception shall be raised.
        country_code : str(2)
            ISO 3166-1 Alpha-2 two letter country code. If the instance does not
            exist in the session then this parameter must be provided to create
            the instance otherwise an exception shall be raised.
        eod_code : str, optional
            EODHistoricalData.com exchange code. See
            https://eodhistoricaldata.com/financial-apis/list-supported-exchanges/
        create : bool, optional
            If `False` then the factory shall expect the specified `Entity` to
            already exist in the session or it shall raise an exception instead
            of creating a first instance.

        To only retrieve an exchange instance from the session one the following
        parameter combinations are required to be specified:

            * The ``mic`` parameter alone.
            * The ``country_code`` and the ``exchange_name``.

        If the instance does not exist then and exception shall be raised.

        To add a new exchange to the session all three parameters must be
        specified.

        Return
        ------
        .Exchange
            The single instance that is in the session.

        See also
        --------
        .Exchange.factory

        """
        # Check if exchange exists in the session and if not then add it.
        try:
            obj = session.query(cls).filter(cls.mic == mic).one()
        except NoResultFound:
            if not create:
                raise FactoryError(
                    'Exchange, mic="{}", not found.'.format(mic))
            if all([country_code, exchange_name]):
                domicile = Domicile.factory(session, country_code)
                obj = cls(exchange_name, domicile, mic, **kwargs)
                session.add(obj)
            else:
                raise FactoryError(
                    'Exchange, mic={}, not found. '
                    'Need `country_code` and `exchange_name` arguments '
                    'to create.'.format(mic))
        else:
            # The country_code and exchange_name are not allowed to Exchange
            if country_code and country_code != obj.domicile.country_code:
                raise ReconcileError(obj, 'country_code')
            if exchange_name and exchange_name != obj.name:
                raise ReconcileError(obj, 'exchange_name')
        finally:
            session.flush()

        return obj
