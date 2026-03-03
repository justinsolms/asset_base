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
the ``Asset._id``'s being the glue. Such a scheme is used with another database
module, the fund ``submissions``. module.

See also
--------
.submissions

"""
import logging
import datetime
import pandas as pd

from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy import MetaData as SQLAlchemyMetaData

from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from asset_base.exceptions import NotSetUp, TimeSeriesNoData, EODSeriesNoData
from asset_base.financial_data import Dump, History, MetaData, Static
from asset_base.common import Base, SQLiteSession, TestSession
from asset_base.entity import Domicile, Exchange
from asset_base.asset import Asset, ExchangeTradeFund, Forex, ListedEquity, Currency, Cash, Listed
from asset_base.time_series_processor import TimeSeriesProcessor


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
        columns = [s._id for s in data_frame.columns]
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
    # TODO: Make a ListedEquityBase parent with ListedEquity next to ExchangeTradeFund child classes. The idea is to use only the leaves orf a hierarchical tree

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
            self.session_obj.close()
            if drop is True:
                # Delegate dropping of the underlying database to the
                # session manager, which knows how to tear down its
                # specific backend (SQLite file, in-memory DB, etc.).
                #
                # NOTE: Manager itself does not implement drop_database;
                # that responsibility lives on the Session wrappers in
                # common.py (TestSession/SQLiteSession via _Session).
                self.session_obj.drop_database()
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
        self, reuse=True, update=True):
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

        # Check for newer data and update the database with API data.
        if update:
            self.update_all()

        # First commit. The update method call below will commit again
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

    def update_all(self):
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

        # TODO: Future security classes place their update_all() methods here.
        ExchangeTradeFund.update_all(self.session)

        # Forex update - based on existing currencies and built in list
        # Forex.foreign_currencies
        Forex.update_all(self.session)

        # TODO: Include Index.update_all

        # Lastly commit all changes to the database
        self.commit()

    def dump(self):
        """Dump reusable market data to disk files.

        This method serialises the classes listed in ``classes_to_dump`` using
        their class-level :meth:`dump` implementations (for example
        :meth:`ListedEquity.dump`). The resulting files can later be consumed
        by :meth:`reuse` to rebuild non-static market data without refetching
        it from upstream data feeds.

        By design, *static* reference data is **not** dumped here. The
        following are always recreated from the built-in static files in
        :mod:`financial_data` and must exist before reuse is attempted:

        - Currency
        - Domicile
        - Exchange
        - Cash

        Currently ``classes_to_dump`` includes only ``ListedEquity`` (and its
        associated time series data: ``ListedEOD``, ``Dividend`` and
        ``Split``).
        """
        for cls in self.classes_to_dump:
            cls.dump(self.session, self.dumper)

    def reuse(self):
        """Reuse previously dumped market data as a database initialisation resource.

        For each class in ``classes_to_dump`` this method calls the
        corresponding class-level :meth:`reuse`. Static reference tables (such
        as currencies, domiciles, exchanges and cash) are **not** populated
        here and must already exist in the database, typically via
        :meth:`set_up`.

        ``reuse`` is primarily intended to speed up creation of a new database
        by avoiding repeated API downloads; it reconstructs objects based on
        business identifiers (for example ISIN) rather than preserving primary
        key values from the database that produced the dump.

        See also
        --------
        .dump
        """
        for cls in self.classes_to_dump:
            # Use uninstantiated class name for logging
            class_name = cls.__name__
            try:
                cls.reuse(self.session, self.dumper)
            except FileNotFoundError:
                logger.info(
                    f"Dump data not found to reuse for class {class_name}.")
            else:
                logger.info(
                    f"Reused dumped data for {class_name}")

    def delete_dumps(self):
        """Delete all dump files while keeping the dump folder.

        This is a convenience wrapper around :meth:`Dump.delete`. It removes
        all dumped DataFrame files but intentionally leaves the underlying dump
        directory in place so that future dump operations can recreate files
        without needing to recreate the directory structure.
        """
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



    def to_common_currency(self, data_frame, currency_ticker):
        """Transform price-like time-series to a common currency.

        Parameters
        ----------
        data_frame : pandas.DataFrame
            An asset price data frame derived from the ``time_series``
            method. Must be price series data. Thus valid
        # FIXME: This method need the Asset polymorph objects and time_series() no longer exists.
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

    def get_time_series_processor(
        self, asset_list, cash_currency_ticker=None, price_item="close"):
        """Get a time series processor for a list of assets.

        Parameters
        ----------
        asset_list : list of asset_base.asset.AssetBase
            List of ``Asset`` (or polymorph) instances.
        cash_currency_ticker : str, optional
            ISO 4217 3-letter currency ticker for the single cash asset to
            include. There must be exactly none or one ``Cash`` asset in this
            currency and its currency must match that of all assets referenced
            in the ``asset_list``. If not provided then no cash asset is
            included.
        price_item : str, optional
            The price item to use for listed assets. These could be 'open',
            'close', 'high' or 'low' depending on the available data. Default is
            'close'.

        Returns
        -------
        TimeSeriesProcessor
            A TimeSeriesProcessor instance with time-series data loaded for all
            assets referenced in ``asset_list`` and the cash asset
            if specified by ``cash_currency_ticker``. The time-series data is
            not yet processed or adjusted for corporate actions. This is left to
            the caller to run the full processing pipeline via
            ``TimeSeriesProcessor.process()`` method.

        Notes
        -----
                - Assets with missing time-series data are skipped and a warning is
                    logged and the method proceeds with the
          remaining assets. If no assets with usable time-series data are found
          then a ``TimeSeriesNoData`` exception is raised.
        - All listed assets and the cash asset must share the same price
            currency. Currently mixed-currency portfolios are rejected with a
            ``ValueError``.
        - The returned TimeSeriesProcessor performs validation, corporate action
          adjustment and resampling on a per-asset basis.

        """
        if not asset_list:
            raise ValueError("Argument `asset_list` may not be empty.")

        # Enforce common currency across all assets (listed + cash)
        currency_tickers = {asset.currency.ticker for asset in asset_list}
        if len(currency_tickers) != 1:
            raise ValueError(
                f"Mixed asset currencies detected {currency_tickers}. "
                "All listed assets and the cash asset must share the "
                "same currency in the current implementation."
            )

        # Resolve the unique Cash asset for the requested currency if a
        # cash_currency_ticker is provided. If not provided then no cash asset
        # is included.
        if cash_currency_ticker is not None:
            try:
                cash_currency = (
                    self.session.query(Currency)
                    .filter(Currency.ticker == cash_currency_ticker)
                    .one()
                )
            except NoResultFound:
                raise TimeSeriesNoData(
                    f"No Currency found for ticker {cash_currency_ticker!r}. "
                    "Ensure static currency data is loaded via Manager.set_up()."
                )
            else:
                cash_assets = (
                    self.session.query(Cash)
                    .filter(Cash._currency_id == cash_currency._id)
                    .all()
                )
                if len(cash_assets) == 0:
                    raise TimeSeriesNoData(
                        "No Cash asset found for currency ticker "
                        f"{cash_currency_ticker!r}. Exactly one Cash asset is required."
                    )
                if len(cash_assets) > 1:
                    raise TimeSeriesNoData(
                        "Multiple Cash assets found for currency ticker "
                        f"{cash_currency_ticker!r}. Exactly one Cash asset is required."
                    )
                cash_asset = cash_assets[0]

            # Enforce common currency between cash asset and listed assets
            currency_tickers.add(cash_asset.currency.ticker)
            if len(currency_tickers) != 1:
                # TODO: In a future implementation we may wish to allow mixed-currency portfolios and perform currency transformation to a common currency within the time series processor. For now we reject mixed-currency portfolios with an error.
                raise ValueError(
                    f"Mixed asset currencies detected {currency_tickers}. "
                    "All listed assets and the cash asset must share the "
                    "same currency in the current implementation."
                )
        else:
            cash_asset = None

        # Build TimeSeriesProcessor objects for listed assets, handling
        # missing time-series data on a per-asset basis.
        tsp_list = []
        for asset in asset_list:
            try:
                tsp = asset.get_time_series_processor(price_item=price_item)
            except EODSeriesNoData:
                logger.warning(
                    "Missing EOD series for asset %s (identity_code=%s). "
                    "Skipping this asset.",
                    asset,
                    asset.identity_code,
                )
                continue
            else:
                tsp_list.append(tsp)
        if not tsp_list:
            raise TimeSeriesNoData(
                "No usable listed assets with time-series data for the "
                "provided asset_list."
            )

        # Combine all listed TimeSeriesProcessors and derive the common
        # date index for building the cash series.
        non_cash_tsp = TimeSeriesProcessor.concat(tsp_list)
        if cash_asset is not None:
            date_index = non_cash_tsp.get_date_index()
            cash_tsp = cash_asset.get_time_series_processor(
                date_index=date_index, price_item="price"
            )
            tsp_all = TimeSeriesProcessor.concat([non_cash_tsp, cash_tsp])
        else:
            tsp_all = non_cash_tsp

        return tsp_all

    def get_asset_dict(self, identity_code_list):
        """Get a dict of assets based on a list of identity codes.

        Parameters
        ----------
        identity_code_list : list of str
            A list of asset identity codes.

        Returns
        -------
        dict
            Mapping of ``identity_code`` strings to ``Asset`` (or
            polymorphic child) instances found in the current session.
            Unknown or malformed identity codes are skipped with a warning.
        """
        # Bulk query all assets at once with optimized polymorphic loading
        from sqlalchemy.orm import with_polymorphic

        # Use with_polymorphic to optimize joined table inheritance queries
        # This did not really speed things up much.
        poly_asset = with_polymorphic(Asset, '*')
        assets = self.session.query(poly_asset).filter(
            poly_asset.identity_code.in_(identity_code_list)
        ).all()

        # Build dict from results
        asset_dict = {asset.identity_code: asset for asset in assets}

        # Check for missing identity codes and log warnings
        found_codes = set(asset_dict.keys())
        requested_codes = set(identity_code_list)
        missing_codes = requested_codes - found_codes

        for missing_code in missing_codes:
            logger.warning(
                "No asset found for identity_code %s", missing_code)

        # Raise an error if no assets were found for any of the provided
        # identity codes
        if not asset_dict:
            raise TimeSeriesNoData(
                "No assets found for the provided identity_code_list."
            )

        return asset_dict


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
