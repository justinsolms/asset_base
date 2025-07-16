#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Object relational mapping module to the ``asset_base`` database.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the asset_base module.
The asset_base module can not be modified, copied and/or
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
import logging
import yaml
import datetime
import pandas as pd

from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy import MetaData as SQLAlchemyMetaData

from sqlalchemy.orm.exc import NoResultFound

from asset_base import get_config_path

from .exceptions import NotSetUp, TimeSeriesNoData
from .financial_data import Dump, History, MetaData, Static
from .common import Base, SQLiteSession, TestSession
from .entity import Domicile, Exchange
from .asset import (
    Asset,
    ExchangeTradeFund,
    Forex,
    ListedEquity,
    Currency,
    Cash,
)


# Get module-named logger.
logger = logging.getLogger(__name__)

# Pull in the meta data
metadata = SQLAlchemyMetaData()


def substitute_security_labels(data_frame, identifier, inplace=False, labels_only=False):
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

    inplace: bool
        If True, modifies the DataFrame in place (do not create a new object).
        Else returns a new (copied) object with the column labels changed. May
        not be used together with labels_only=True.
    labels_only: bool
        If True, returns only the list of column labels and not the DataFrame.
        May not be used together with inplace=True.

    Returns
    -------
    pandas.DataFrame, list or None
        See arguments `inplace` and `labels_only`.

    """
    # Pick column label identifier.
    if identifier == "id":
        columns = [s.id for s in data_frame.columns]
    elif identifier == "identity_code":
        # Translation of column id to codes.
        columns = [s.identity_code for s in data_frame.columns]
    elif identifier == "ticker":
        # Translation of column id to codes.
        columns = [s.ticker for s in data_frame.columns]
    elif identifier == "isin":
        # Translation of column id to codes.
        columns = [s.isin for s in data_frame.columns]
    elif identifier == "name":
        # Translation of column id to names.
        columns = [s.name for s in data_frame.columns]
    else:
        raise ValueError('Unexpected value for "identifier" argument.')

    if inplace and labels_only:
        raise ValueError("Cannot use both inplace=True and labels_only=True.")
    elif labels_only:
        return columns
    elif inplace:
        data_frame.columns = columns
    else:
        data_frame = data_frame.copy()
        data_frame.columns = columns
        return data_frame


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

    __tablename__ = "meta"

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
        return f"Meta name={self.name}, value={self.value}"

    def __repr__(self):
        """Return the official string output."""
        return f"Meta(name={self.name}, value={self.value})"


# TODO: Consider converting flush commands to try-commit-exception-rollback


class ManagerBase(object):
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

    testing : bool
        Set to `True` for testing.

    """

    # These must only be `Asset` polymorphs.
    classes_to_dump = [ListedEquity]

    def __init__(self, dialect="sqlite", testing=False):
        """Instance initialization.

        Parameters
        ----------
        dialect : {'sqlite', 'memory'}, optional
            The database dialect is the specific underlying database.
        testing : bool, optional
            Set to `True` for testing. This controls specifics in a way that
            avoids testing data clashes with operational data. This only affects
            the 'sqlite' `dialect` argument option.
        """
        self._dialect = dialect
        self.testing = testing

        self._make_session()

        # Data dumper - dumps to dump folder - indicate testing or not.
        self.dumper = Dump(testing=self.testing)

    def __del__(self):
        """Destruction."""
        self.close()

    def delete(self):
        """Delete the entire database."""
        self.close(drop=True)

        # If the database exists then delete it.
    def close(self, drop=False):
        """Close the database session and dispose of the engine.

        Parameters
        ----------
        drop : bool, optional
            If `True` then the database is dropped and deleted. If `False` then
            the database is not deleted but the session and engine are closed.

        """
        if hasattr(self, "session_obj") and self.session_obj is not None:
            self.session_obj.close(drop=drop)
            del self.session_obj
        if hasattr(self, "session"):
            # Also delete the _make_session convenience attribute
            del self.session  # See _make_session convenience attribute

    def _make_session(self):
        """Make database sessions in either sqlite, mysql or memory."""
        if self._dialect == "memory":
            self.session_obj = TestSession()
        elif self._dialect == "sqlite":
            self.session_obj = SQLiteSession(testing=self.testing)

        # A convenience attribute to the SQLAlchemy session object.
        self.session = self.session_obj.session

    def commit(self):
        """Session try-commit, exception-rollback."""
        try:
            self.session.commit()
        except Exception as ex:
            logger.critical("Commit failed - rolling back.")
            self.session.rollback()
            logger.info("Rolled back.")
            # Rethrow the exception
            raise ex

    def set_up(
        self, reuse=True, update=True, _test_isin_list=None, _test_forex_list=None
    ):
        """Set up the database for operations.

        Parameters
        ----------
        reuse : bool
            When `True` then previous dumped database content will be reused to
            initialise the database.
        update : bool
            When `True` then API feeds will be checked for newer data.

        """
        # Create a new database and engine if not existing
        if not hasattr(self, "session_obj"):
            self._make_session()

        # Record creation moment as a string (item, value) pair if it does not
        # already exist.
        try:
            meta = self.session.query(Meta).filter_by(name="set_up_date").one()
        except NoResultFound:
            set_up_date = datetime.datetime.now().isoformat()
            self.session.add(Meta("set_up_date", set_up_date))
        else:
            set_up_date = meta.value
        finally:
            logger.info(f"Set-up date of database is {set_up_date}")

        # Set up static data
        static_obj = Static()
        Currency.update_all(self.session, get_method=static_obj.get_currency)
        Domicile.update_all(self.session, get_method=static_obj.get_domicile)
        Exchange.update_all(self.session, get_method=static_obj.get_exchange)

        # Create all cash currency instances for every domicile
        Cash.update_all(self.session)

        # Reuse old dumped/cached data
        if reuse:
            self.reuse()

        # Update all from API feeds
        if update is True:
            self.update(
                _test_isin_list=_test_isin_list, _test_forex_list=_test_forex_list
            )

        # Try to commit all results
        self.commit()

    def tear_down(self, delete_dump_data=False):
        """Tear down the environment for operation of the module.

        Parameters
        ----------
        delete_dump_data : bool, optional
            If `True` then data is not dumped and the dump folder and its
            contents are deleted. Warning: do not use this unless you are really
            sure you wish to delete all your reusable data sources. If `False`
            then re-use data is first dumped before the database is torn down.
        """
        if delete_dump_data:
            logger.warning("Deleting reusable dump files.")
            self.delete_dumps()
            logger.warning("Successfully deleted dump files.")
        else:
            # Dump reusable data if any.
            try:
                self.dump()
            except Exception as ex:
                logger.critical("Dump asset_base failed. Tear-down aborted!!!")
                raise ex
            else:
                logger.info(
                    "Successful dump of  reusable asset_base data which can be used to shorten set-up time")

        # Delete database
        self.close(drop=True)

    def update(self, _test_isin_list=None, _test_forex_list=None):
        """Update all non-static data.

        Uses the ``.financial_data`` module as the data source.
        """
        # Check if the database has been set up.
        try:
            meta = self.session.query(Meta).filter(Meta.name == "set_up_date").one()
        except NoResultFound:
            raise NotSetUp(
                "Database has not been set up. Please call set_up() first."
            )

        # Check for newer securities data and update the database
        fundamentals = MetaData()
        history = History()
        # NOTE: Future security classes place their update_all() methods here.
        # NOTE: ListedEquity.update_all() here
        ExchangeTradeFund.update_all(
            self.session,
            get_meta_method=fundamentals.get_etfs,
            get_eod_method=history.get_eod,
            get_dividends_method=history.get_dividends,
            _test_isin_list=_test_isin_list,  # Hidden arg. For testing only!
        )

        # Forex update - based on existing currencies and built in list
        # Forex.foreign_currencies
        Forex.update_all(
            self.session,
            get_forex_method=history.get_forex,
            _test_forex_list=_test_forex_list,  # Hidden arg. For testing only!
        )

        # TODO: Include Index.update_all

    def dump(self):
        """Dump re-usable content to disk files.

        The purpose of the dump files is to provide a convenience reusable data
        source for initialising the ``asset_base`` database without
        necessitating a lengthy download of data from the data feeds (via the
        ``financial_data` module). Instead time may be save as most of the
        previously dumped data should be reused and only a small fraction of new
        data download form feeds.

        The dump shall include data from the classes:
        - ListedEquity (and its time series data: ListedEOD and Dividend)

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
        # TODO: FInd and make clear ho EOD and Dividend data is reused.
        # TODO: Reuse FOREX data
        """Reuse dumped data as a database initialization resource.

        See also
        --------
        .dump
        """
        for cls in self.classes_to_dump:
            try:
                cls.reuse(self.session, self.dumper)
            except FileNotFoundError:
                logger.info(
                    f"Dump data not found to reuse for class {cls._class_name()}.")
            else:
                logger.info(
                    f"Reused dumped data for {cls._class_name()}")

    def delete_dumps(self):
        """Delete dumped data folder."""
        self.dumper.delete()

    def get_meta(self):
        """Get a dictionary of asset_base meta-data.

        Returns
        -------
        dict
            A dictionary of meta data strings.
        """
        data = [(str(item.name), str(item.value)) for item in self.session.query(Meta)]
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
        """
        if isinstance(id, list):
            # Get the list of matching funds and construct a new list.
            entities = self.session.query(Asset).filter(Asset.id.in_(id))
            return dict([(asset.id, asset) for asset in entities])
        else:
            raise ValueError(
                "Argument `id` must be a list of asset id numbers."
            )

    def time_series(
        self,
        asset_list,
        series="price",
        price_item="close",
        return_type="price",
        identifier="asset",
        tidy=True,
        date_index=None,
    ):
        """Return historic price or return, time-series for a list of assets.

        TODO: Remove `series` argument and use to get price series only

        Note
        ----
        The values of `Cash` entities shall always be equivalent to a price of
        1.0 for all dates in the date range.

        Parameters
        ----------
        asset_list : list of Asset (or polymorph class) instances
            A list of securities or assets for which time series are required.
        series : str, optional
            Which security series:

            'price':
                The security's periodic trade price.
            'dividend':
                The annualized distribution yield.
            'volume':
                The volume of trade (total units of trade) in the period.
        price_item : str, optional
            The specific item of price. Only valid for the `price` type:

            'close' :
                The period's close price.
            'open' :
                The period's open price.
            'low' :
                The period's lowest price.
            'high' :
                The period's highest price.
        return_type : str, optional
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
                That which will be returned by the ``asset.Asset.identity_code``
                attribute.
        tidy : bool, optional
            When ``True`` then prices are tidied up by removing outliers.
        date_index : pandas.DatetimeIndex, optional
            If there are non-cash securities specified in the `asset_list` then
            this argument is overridden by the union of the date index (the
            `pandas.DatetimeIndex`) of all the non-`Cash` security time-series.
            If the `asset_list` argument specifies only `Cash` securities then
            this data range is not optional and is required. It could be
            provided by the the index of another `time_series` result. See
            documentation on ``Cash.time_series`` for the explanation.

        Returns
        -------
        pandas.DataFrame
            A column of data for each  ``.asset.Asset`` instance (or polymorph)
            in the ``asset-list`` argument. The column labels are the
            ``.asset.Asset`` instances.

        Raises
        ------
        ValueError
            Argument id_list may not be empty.
        Exception
            Expected non-cash securities in asset_list.

        See also
        --------
        Cash.time_series

        """
        if len(asset_list) == 0:
            raise ValueError("Argument `asset_list` may not be empty.")

        # Get a list of cash securities
        cash_list = [item for item in asset_list if isinstance(item, Cash)]

        # Get a list of non-cash securities
        non_cash_list = [asset for asset in asset_list if not isinstance(asset, Cash)]

        #  A date-index must be provided to specify the cash data date range if
        #  there are no non-cash securities from which the date range may be
        #  derived.
        if len(non_cash_list) == 0 and date_index is None:
            raise ValueError(
                "Expected non-cash securities in asset_list or a date_index "
                "argument to specify the date range for the cash securities."
            )

        data_list = list()
        if len(non_cash_list) > 0:
            # Create a pandas.DataFrame of non-cash securities
            data_list = list()
            # For non-Cash entities
            for asset in non_cash_list:
                # Slip and warn for absent time-series.
                try:
                    data = asset.time_series(series, price_item, return_type)
                except TimeSeriesNoData as ex:
                    logger.warning(ex)
                else:
                    data_list.append(data)
            data = pd.concat(data_list, axis=1, sort=True)
            # Any data_index  argument is ignored as non-cash security data date
            # range takes precedence.
            data_list = [data]  # List for appending Cash securities to.

        # For all non-Cash entities. We need the previous data DatetimeIndex to
        # construct Cash time series. See docs.
        date_index = data.index
        for asset in cash_list:
            data = asset.time_series(date_index)
            data_list.append(data)
        # Concatenate the separate data in the list into one pandas.DataFrame.
        data = pd.concat(data_list, axis=1, sort=True)

        # Assure ascending date index
        data.sort_index(inplace=True)

        # Warning if a dataframe has mixed currency time series.
        if len(set(s.currency for s in data.columns)) > 1:
            logger.warning("The DataFrame data is of mixed currencies.")

        # Replace column labels as specified by the identifier arg
        # TODO: Replace with a match statement
        if identifier == "asset":
            id_dict = {}
        elif identifier == "id":
            id_dict = {asset: asset.id for asset in asset_list}
        elif identifier == "isin":
            id_dict = {asset: asset.isin for asset in asset_list}
        elif identifier == "ticker":
            id_dict = {asset: asset.ticker for asset in asset_list}
        elif identifier == "identity_code":
            id_dict = {asset: asset.identity_code for asset in asset_list}
        else:
            raise ValueError(f"Unexpected `identifier` argument `{identifier}`.")
        data = data.rename(columns=id_dict)
        data.columns.name = identifier

        # Return all time series in one pandas.DataFrame.
        return data

    def to_common_currency(self, data_frame, currency_ticker):
        """Transform price-like time-series to a common currency.

        Parameters
        ----------
        data_frame : pandas.DataFrame
            An asset price data frame derived from the ``time_series``
            method. Must be price series data. Thus valid
        currency_ticker : str(3), optional
            ISO 4217 3-letter currency code of the desired price currency.

        Returns
        -------
        pandas.DataFrame
            The price data frame with prices transformed to the new currency of
            the ``currency_ticker`` argument.

        Warning
        -------
        It is _incorrect_ to transform anything but a price to a new price
        currency and transforming returns or volume series is incorrect. Of
        course a dividend series is also a price-like series and may be
        transformed.

        Warning
        -------
        It shall not be possible to use this method with data from
        ``time_series`` that has had the data column labels modified using some
        method such as the ``replace_time_series_labels`` method. Therefore
        apply please apply the ``to_common_currency`` method before applying the
        ``replace_time_series_labels`` method.

        Warning
        -------
        In the current implementation if the the column labels are
        ``.asset.Asset`` instances, these shall retain their original
        ``.asset.Asset.currency`` even if the ``currency`` argument specifies a
        new currency. In a follow up implementation this may change through the
        use of``sqlalchemy.orm.make_transient()`` where the column label's
        ``.asset.Asset.currency`` will reflect the new currency.

        """
        new = dict()
        # Get the data_frame column currencies and the corresponding forex
        foreign_tickers = [asset.currency.ticker for asset in data_frame.columns]
        foreign_tickers = list(set(foreign_tickers))  # Make list of unique
        forex = Forex.get_rates_data_frame(
            self.session, currency_ticker, foreign_tickers
        )
        # Match index of rate with index of series for correct division
        # later
        data_index = data_frame.index.copy()  # For reindex back to data index
        common_index = data_frame.index.union(forex.index)
        data_frame = data_frame.reindex(index=common_index, method="ffill")
        forex = forex.reindex(index=common_index, method="ffill")
        # Transform each column as per its asset (see column label) currency
        for asset, series in data_frame.items():
            rate = forex[asset.currency.ticker]
            new[asset] = series / rate  # Inverse rate
        data_frame = pd.DataFrame(new)
        # Index back to original data_frame index
        data_frame = data_frame.reindex(index=data_index)

        return data_frame

    # TODO: Add a method to transform and force a single currency within a
    # dataframe of mixed currency time series.

    # TODO: Add a method to flag mixed currencies==True that a dataframe has
    # mixed currency time series.


class Manager(ManagerBase):
    """A context manager that manages the AssetBase session opening and closing.

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
