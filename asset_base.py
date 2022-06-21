#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Object relational mapping module to the ``asset_base`` database.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the fundmanage module.
The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

FIXME: Re-write this doc post split of Entity and Asset class inheritance

It is natural to think of anything as an entity. We assert the following
paradigm. A person is a natural person is an entity (Currently persons are not
included in this version of the ``asset_base`` module). A legally registered
company is a legal entity.

We also wish to express relationships between entities. A person entity may own
a company entity or a stock entity which is a type of asset or share entity. We
shall dispense with over-use of the word entity form here on. A company may hold
stocks or other companies. Companies may issue stocks or bonds. Stocks and bonds
may list on exchange companies. Clearly there are hierarchies of entity types
and non-hierarchical relationships between some of these entity types.

With this view we have evolved a hierarchy, and set of relationships, of entity
types, that in our view represent our world and are encoded in this module.
These types naturally map to a set of object types in an object oriented (or OO)
hierarchy of software classes.

As we wish to gather large amounts of data on large amounts of entities our view
would be well placed in a database of sorts. For this mapping the SQL language
and its implementations come to mind. However, in our experience, the rapid
mapping and development of such a hierarchy in SQL is tedious. We took the
Object relational mapping (or ORM) approach due to its intuitive OO style and
automatically generated underlying SQL tables mapping. In this we have the best
of both worlds; to have our cake and eat it; so to speak.

What follows in this module is a mapping of useful entities and the data that
describes these entities; such as a companies domicile and name or a share's
domicile, name issuer company and exchange of listing, its ticker, ISIN, and so
on. We shall refer to this as entity meta-data, or static-data. The reason is
that the actual data on an entity such as a company share is the time-series
history of close prices, the current earnings to book ratio or the current cash
to debt ratio, and so on. These are the evolving data whereupon investment
decisions are made. However it is still important to know what the relationships
of entities are; such as an exchange traded fund (or ETF) which is issued in a
certain size batch by a company, listed on an exchange, and very importantly
holds a large set of underlying financial security entities in whole or in part,
in the form of it's relationship with a basket of these security entities in a
fund entity; with the additional fact that it is a type of listed equity. This
is why the relationships and hierarchies matter.

It is part of the To-Do list to bring natural entities (natural persons) into
this view. However, it may be practical to keep them in a separate schemas with
the ``Asset.id``'s being the glue. Such a scheme is used with another database
module, the fund ``submissions``. module.

See also
--------
.submissions

"""
import os
import sys
import yaml
import datetime
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from sqlalchemy import String
from sqlalchemy import MetaData, Column

from sqlalchemy_utils import drop_database
from sqlalchemy_utils import create_database
from sqlalchemy_utils import database_exists
from sqlalchemy.orm.exc import NoResultFound

from asset_base.common import Base
from asset_base.entity import Domicile, Exchange
from asset_base.asset import ListedEquity, Currency, Cash
from asset_base.exceptions import TimeSeriesNoData

import asset_base.financial_data as fd

# Get module-named logger.
import logging
logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Pull in the meta data
metadata = MetaData()


def replace_time_series_labels(data_frame, identifier, inplace=False):
    """Replace time series column labels with the specified identifier.

    Parameters
    ----------
        data_frame: pandas.DataFrame
            The data in which column labels must be replaced.
        identifier : str
            Security identifiers are used to label the data columns in the
            returned data, the choice of which is specified by the parameter
            values:

            'id':
                Uses the security (asset) ``id`` number attribute.
            'identity_code':
                Uses the security (asset) ``identity_code`` attribute.
            'ticker':
                Uses the security (asset) ``ticker`` attribute.
            'isin':
                Uses the security (asset) ``isin`` attribute.
            'name':
                Uses the security (asset) ``name`` attribute.
            'alt_name':
                Uses the alternative security (asset) ``alt_name``
                attribute.

    inplace: bool
        If True, modifies the DataFrame in place (do not create a new object).

    Returns
    -------
    DataFrame or None
        Changed row labels or None if ``inplace=True``.

    """
    if not inplace:
        data_frame = data_frame.copy()

    # Pick column label identifier.
    if identifier == 'id':
        columns = [s.id for s in data_frame.columns]
    elif identifier == 'identity_code':
        # Translation of column id to codes.
        columns = [s.identity_code for s in data_frame.columns]
    elif identifier == 'ticker':
        # Translation of column id to codes.
        columns = [s.ticker for s in data_frame.columns]
    elif identifier == 'isin':
        # Translation of column id to codes.
        columns = [s.isin for s in data_frame.columns]
    elif identifier == 'name':
        # Translation of column id to names.
        columns = [s.name for s in data_frame.columns]
    elif identifier == 'alt_name':
        # Translation of column id to names.
        columns = [s.alt_name for s in data_frame.columns]
    else:
        raise ValueError('Unexpected value for "identifier" argument.')

    data_frame.columns = columns

    if not inplace:
        return data_frame
    else:
        return None


class Meta(Base):
    """Meta data or information about the database.

    Stored as string names with associated string values.

    Parameters
    ----------
    name : string
        The parameter by name.
    value : string
        The parameter value in string representation.

    Attributes
    ----------
    name : string
        The parameter by name.
    value : string
        The parameter value in string representation.


    See Also
    --------
    .Model

    """

    __tablename__ = 'meta'

    # Parameter name string.
    name = Column(String(32), primary_key=True)

    # Parameter value string.
    value = Column(String(32), nullable=False)

    def __init__(self, name, value):
        """Instance initialization."""
        self.name = name
        self.value = value

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return 'Meta data "%s" = "%s"' % (self.name, self.value)

    def __repr__(self):
        """Return the official string output."""
        return '<Meta data:"%s"="%s">' % (self.name, self.value)


# TODO: Consider converting flush commands to try-commit-exception-rollback


class AssetBase(object):
    """Database set-up, destruction, initialization, management, maintenance.

    Parameters
    ----------
    dialect : str
        The SQL database dialect to be used:

        'memory':
            A memory database session. Is destroyed after use.
        'sqlite':
            A SQLite session.
        'mysql':
            A MySQL session.

    """
    # These must only be Asset polymorphs
    classes_to_dump = [ListedEquity]

    def __init__(self, dialect='sqlite', testing=False):
        """Instance initialization.

        Parameters
        ----------
        dialect : {'sqlite', 'mysql', 'memory'}, optional
            The database dialect is the specific underlying database.
        testing : bool, optional
            Set to `True` for testing. This controls specifics in a way that
            avoids testing data clashes with operational data.
        """
        # Open main configuration YAML file and convert to a dict.
        path = os.path.dirname(os.path.realpath(__file__))
        with open(path + '/' + 'conf.yaml', 'r') as stream:
            self._config = yaml.full_load(stream)

        # Path for data dumps.
        tmp_path = self._config['directories']['working']['tmp']
        self._tmp_path = os.path.expanduser(tmp_path)  # Full path.

        self._dialect = dialect

        # Create a new database and engine if not existing
        if not hasattr(self, 'session'):
            self.new_database()

        # Data dumper - dumps to dump folder
        self.dumper = fd.Dump(testing=testing)

    def commit(self):
        """Session try-commit, exception-rollback."""
        try:
            self.session.commit()
        except Exception as ex:
            logger.error('Commit failed - rolling back.')
            self.session.rollback()
            logger.info('Rolled back.')
            raise ex

    def new_database(self):
        self._db_name = 'asset_base'
        # Select database platform.
        if self._dialect == 'mysql':
            # MySQL URL.
            db_url = self._config['backends']['database']
            db_url = db_url['mysql']['asset_base'] % self._db_name
        elif self._dialect == 'sqlite':
            # Construct SQLite file name with path expansion for a URL.
            self._db_name = 'fundmanage.%s.db' % self._db_name
            cache_path = self._config['directories']['working']['cache']
            cache_path = os.path.expanduser(cache_path)  # Full path.
            db_file_name = '%s/%s' % (cache_path, self._db_name)
            db_url = 'sqlite:///' + db_file_name
            self._db_name = db_file_name
        elif self._dialect == 'memory':
            db_url = 'sqlite://'
        else:
            raise ValueError('Unrecognised dialect "%s"' % self._dialect)

        self.db_url = db_url

        # Create a database engine.
        self.engine = create_engine(db_url)

        # Create an empty database with all tables if it doesn't already exist.
        if not database_exists(self.db_url) or self._dialect == 'memory':
            try:
                create_database(db_url)
                Base.metadata.create_all(self.engine)
            except Exception as ex:
                drop_database(db_url)
                raise ex

        self.session = Session(self.engine, autoflush=True, autocommit=False)
        logger.info('New database, engine and tables with URL "%s"' % db_url)

    def dump(self):
        """Dump re-usable content to disk files.

        The purpose of the dump files is to provide a convenience reusable data
        source for initialising the ``asset_base`` database without
        necessitating a lengthy download of data from the data feeds (via the
        ``financial_data` module). Instead time may be save as most of the
        previously dumped data should be reused and only a small fraction of new
        data download form feeds.

        The dump shall include data from the classes:
        - ListedEquity (and its time series data: TradeEOD and Dividend)

        This excludes the following data items which are always available as
        static data through the ``financial_data.Static`` class or as
        derived data form the static data:
        - Currency
        - Domicile
        - Exchange
        - Cash
        """
        for cls in self.classes_to_dump:
            cls.dump(self.session, self.dumper)

    def reuse(self):
        """Reuse dumped data as a database initialization resource.

        See also
        --------
        .dump
        """
        for cls in self.classes_to_dump:
            try:
                cls.reuse(self.session, self.dumper)
            except fd.DumpReadError:
                logger.info(f'Unavailable dump data for {cls._class_name}')

    def delete_dumps(self, delete_folder=True):
        """Delete dumped data folder

        The dump data folder and its contents contain dumped asset_base data
        which can be reused as a database initialization resource.

        Parameters
        ----------
        delete_folder : bool, optional
            When set to `True` (default) then the dump folder and its contents
            are  deleted too.  If set `False` then the folder is kept but its
            content are deleted.
        """
        self.dumper.delete(delete_folder=delete_folder)

    def set_up(self, reuse=True, update=True, _test_isin_list=None):
        """Set up the database for operations.

        Parameters
        ----------
        reuse : bool
            When `True` then previous dumped database content will be reused to
            initialise the database.
        update : bool
            When `True` then feeds will be checked for newer data.

        """
        # Create a new database and engine if not existing
        if not hasattr(self, 'session'):
            logger.info('New database session created.')
            self.new_database()

        # Record creation moment as a string (item, value) pair if it does not
        # already exist.
        try:
            meta = self.session.query(Meta).filter(
                Meta.name == 'set_up_date').one()
        except NoResultFound:
            set_up_date = datetime.datetime.now().isoformat()
            self.session.add(Meta('set_up_date', set_up_date))
        else:
            set_up_date = meta.value
        finally:
            logger.info(f'Set-up date of database is {set_up_date}')

        # Set up static data
        static_obj = fd.Static()
        Currency.update_all(self.session, get_method=static_obj.get_currency)
        Domicile.update_all(self.session, get_method=static_obj.get_domicile)
        Exchange.update_all(self.session, get_method=static_obj.get_exchange)

        # Create all cash currency instances for every domicile
        Cash.update_all(self.session)

        # Reuse old dumped/cached data
        if reuse:
            self.reuse()

        # Check for newer securities data and update the database
        fundamentals = fd.SecuritiesFundamentals()
        history = fd.SecuritiesHistory()
        ListedEquity.update_all(
            self.session,
            get_meta_method=fundamentals.get_securities,
            get_eod_method=history.get_eod,
            get_dividends_method=history.get_dividends,
            _test_isin_list=_test_isin_list,  # Hidden arg. For testing only!
            )
        # NOTE: Future security classes place their update_all() methods here.

    def tear_down(self, delete_dump_data=False):
        """Tear down the environment for operation of the module.

        Parameters
        ----------
        delete_dump_data : bool, optional
            If `True` then data is not dumped and the dump folder and its
            contents are deleted. Warning: do not use this unless you are really
            sure you wish to delete all your reusable data sources.
        """
        # Dump reusable data. Abort with CRITICAL logging if failed.
        if delete_dump_data:
            logger.info('Deleted dump files and folder.')
            self.delete_dumps(delete_folder=True)
        else:
            try:
                self.dump()
            except Exception as ex:
                logger.critical('Dump asset_base failed. Tear-down aborted!!!')
                raise ex
            else:
                logger.critical('Dump important asset_base data for re-use.')

        # Delete database
        if database_exists(self.engine.url):
            self.session.close()
            self.engine.dispose()
            drop_database(self.engine.url)
            # Delete specific attributes
            del self.db_url
            del self.engine
            del self.session
            logger.info(
                'Dropped database and closed session and engine')

    def get_meta(self):
        """Get a dictionary of asset_base meta-data.

        Returns
        -------
        dict
            A dictionary of meta data strings.
        """
        data = [(str(item.name), str(item.value))
                for item in self.session.query(Meta)]
        return dict(data)

    def get_dict(self, id):
        """Get a dictionary of assets.

        The returned dictionary items will be polymorphic instances of the
        assets specified by the list of asset id numbers.

        Parameters
        ----------
        id : list
            A list if database session `Asset.id` id numbers of the required
            database assets. See `.Asset`.

        Return
        ------
        dict
            A dictionary of assets with the specified id numbers. The id
            numbers are the keys of the dictionary.

        See also
        --------
        .EntityBase.get_time_series_data_frame
        """
        if isinstance(id, list):
            # Get the list of matching funds and construct a new list.
            entities = self.session.query(Asset).filter(Asset.id.in_(id))
            return dict([(asset.id, asset) for asset in entities])
        else:
            raise Exception('Expected a list of asset id(s).')

    def time_series(self, asset_list,
                    series='price', price_item='close', return_type='price',
                    tidy=True, identifier='id', date_index=None):
        """Retrieve historic time-series for a list of entities.

        Note
        ----
        The values of `Cash` entities shall always be equivalent to a price of
        1.0 for all dates in the date range.

        Parameters
        ----------
        asset_list : list of Asset (or polymorph class) instances
            A list of securities or assets for which time series are required.
        series : str
            Which security series:

            'price':
                The security's periodic trade price.
            'distribution':
                The annualized distribution yield.
            'volume':
                The volume of trade (total value of trade) in the period.
        price_item : str
            The specific item of price. Only valid for the `price` type:

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
                The price period-on-period return series including the extra
                yield due to distribution paid.
            'total_price':
                The price period-on-period price series inclusive of the extra
                yield due to dividends paid. The total_price series start value
                is the same as the price start value.
        tidy : bool
            When ``True`` then prices are tidied up by removing outliers.
        date_index : pandas.DatetimeIndex, optional
            If there are non-cash securities specified by `id_list` then this
            argument is overridden by the resulting union of
            `pandas.DatetimeIndex` of the time-series for all specified non-cash
            securities. This data range is not optional and is required if the
            `id_list` argument specifies only `Cash` security ids. See
            documentation on `Cash.time_series`.

        Raises
        ------
        ValueError
            Argument id_list may not be empty.
        ValueError
            Argument id_list has entries not found in session.
        ValueError
            Argument id_list must contain at least one non-cash security.

        """
        if len(asset_list) == 0:
            raise ValueError('Argument id_list may not be empty.')

        # Get a list of cash securities
        cash = [item for item in asset_list if isinstance(item, Cash)]

        # Get a list of non-cash securities
        non_cash = [asset for asset in asset_list if not isinstance(asset, Cash)]

        #  A date-index must be provided to specify the cash data date range if
        #  there are no non-cash securities from which the date range may be
        #  derived.
        if len(non_cash) == 0 and date_index is None:
            raise Exception('Expected non-cash securities in asset_list.')

        data_list = list()
        if len(non_cash) > 0:
            # Create a pandas.DataFrame of non-cash securities
            data_list = list()
            # For non-Cash entities
            for asset in non_cash:
                # Slip and warn for absent time-series.
                try:
                    data = asset.time_series(
                        series, price_item, return_type, tidy)
                except TimeSeriesNoData:
                    logger.warning(
                        f'No time-series for security {asset.identity_code}')
                else:
                    data_list.append(data)
            data = pd.concat(data_list, axis=1, sort=True)
            # Any data_index  argument is ignored as non-cash security data date
            # range takes precedence.
            date_index = data.index
            data_list = [data]  # For appending to.

        # For all non-Cash entities. We need the previous data DatetimeIndex to
        # construct Cash time series. See docs.
        for asset in cash:
            data = asset.time_series(date_index, identifier)
            data_list.append(data)
        # Concatenate the separate data in the list into one pandas.DataFrame.
        data = pd.concat(data_list, axis=1, sort=True)

        # Warning if a dataframe has mixed currency time series.
        if len(set(s.currency for s in data.columns)) > 1:
            logger.warning('The DataFrame data is of mixed currencies.')

        # Return all time series in one pandas.DataFrame.
        return data

    # TODO: Add a method to transform and force a single currency within a
    # dataframe of mixed currency time series.

    # TODO: Add a method to flag mixed currencies==True that a dataframe has
    # mixed currency time series.
