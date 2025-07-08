#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Support for time series data.

Note
----
This module provides all price units in `units` and not `cents`. In this
module's several `to_dict` methods the ``asset.Base.quote_units`` bool attribute
of the related security object is used to determine if conversion from cents to
units is necessary. Nonetheless the underlying data in the time-series database
tables is stored as indicated by ``asset.Base.quote_units`` bool attribute

"""
# Allows  in type hints to use class names instead of class name strings
from __future__ import annotations

import sys
import pandas as pd

from sqlalchemy import Float, Integer, String, Date
from sqlalchemy import MetaData, Column, ForeignKey
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

# Used to avoid ImportError (most likely due to a circular import)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .asset import Asset

from .common import Base
from .financial_data import Dump

# Get module-named logger.
import logging

logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Pull in the meta data
metadata = MetaData()


class TimeSeriesBase(Base):
    """ "Common time-series capabilities.

    Note
    ----
    This is an abstract base class.

    Note
    ----
    All documented ``Listed`` documented class references may also refer to
    any of ``Listed`` child classes.


    Parameters
    ----------
    base_obj : .asset.Base (or polymorph child class)
        The instance the time series data belongs to.
    date_stamp : datetime.date
        The end-of-day (EOD) data date stamp.
    """

    __tablename__ = "time_series_base"

    # Polymorphism discriminator.
    _discriminator = Column(String(32))

    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
        "polymorphic_on": _discriminator,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    """int: Primary key."""

    __table_args__ = (UniqueConstraint("_discriminator", "_asset_id", "date_stamp"),)

    # Foreign key giving the ``Asset`` class a time series capability
    _asset_id = Column(Integer, ForeignKey("asset_base.id"), nullable=False)
    base_obj = relationship("AssetBase", back_populates="_series", foreign_keys=[_asset_id])

    date_stamp = Column(Date, nullable=False)
    # TODO: Consider making this part of the primary keys
    """datetime: EOD date."""

    date_column_names = ["date_stamp"]
    """list: Columns that must be exported externally as pandas.Timestamp."""

    def __init__(self, base_obj, date_stamp):
        """Instance initialization."""
        self.base_obj = base_obj
        self.date_stamp = date_stamp

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        name = self._class_name()
        date = self.date_stamp
        id_code = self.base_obj.identity_code

        return f"Time series:{name}, date stamp:{date}, asset:{id_code}"

    def __repr__(self):
        """Return the official string output."""
        name = self._class_name()
        asset = self.base_obj
        date = self.date_stamp

        return f"{name}(asset={asset!r}, date_stamp={date})"

    @classmethod
    def _class_name(cls):
        return cls.__name__

    @classmethod
    def _get_last_date(cls, security):
        """Get the time series last date maintained by the security.

        Override for other time series as needed.
        """
        return security.get_last_eod_date()

    @classmethod
    def from_data_frame(cls, session, asset_class: Asset, data_frame):
        """Create multiple class instances in the session from a dataframe.

        This method updates all of a specified time series aggregated by the
        ``Listed``  or it's child classes.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        asset_class : .asset.Base (or child class)
            The ``Base`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).
        data_frame : pandas.DataFrame
            A ``pandas.DataFrame`` with columns of the same name as this
            class' constructor method arguments, with the exception that instead
            of a column named ``asset``,
            column with the ISIN number of the ``Listed`` instance.

        """

        # Check for zero rows of data
        if data_frame.empty:
            # No data so return
            return

        # The goal is to substitute the `key_code_name` column with the
        # `Asset.id`.
        key_code_name = asset_class.key_code_name
        # Get Asset.key_code to Asset.id translation table
        key_code_id_table = asset_class.key_code_id_table(session)

        data_table = data_frame

        # Guarantee uniqueness in a copy of the data
        data_frame.drop_duplicates(["date_stamp", key_code_name], inplace=True)

        # Guarantee date ranking of the data
        data_table.sort_values(by="date_stamp")

        # Replace pesky pd.NaT with None. Else SqlAlchemy DateTime columns
        # throw (exceptions.TypeError) %d format: a number is required, not
        # float
        data_table.replace({pd.NaT: None}, inplace=True)

        # Join to create a new extended instance_table with the security column.
        # Only for time series instances (left join).
        # FIXME: Warn or raise if left and right are not congruent
        data_table = data_table.merge(key_code_id_table, on=key_code_name, how="left")
        data_table.drop(columns=key_code_name, inplace=True)

        instances_list = list()
        data_table.set_index(["id", "date_stamp"], inplace=True, drop=True)
        # Iterate over all Asset polymorph instances

        # Determine which, if any, security id's are present in the data.
        id_list = data_table.index.to_frame(index=False).id.drop_duplicates().to_list()
        # Avoid empty data edge case - with certain date ranges in a data fetch,
        # there may be no new data to be found
        if len(id_list) == 0:
            # Nothing to process so just return
            return
        # Fetch the relevant securities
        security_list = (
            session.query(asset_class).filter(asset_class.id.in_(id_list)).all()
        )
        # Add data to each security's time series' asset_class
        for security in security_list:
            # Get the security's time series.
            series = data_table.loc[security.id]
            # Sort by ascending date_stamp. Use a copy (inplace=False) to avoid
            # a SettingWithCopyWarning
            series.sort_index(inplace=True)
            # Reset date_stamp index making it a column
            series.reset_index(inplace=True)

            # Keep only new dated data in the data_table. Due to the behaviour
            # of financial data API services very often returns data which may
            # include data falling on a date that has already been stored. This
            # may lead to duplicate data which we wish to avoid.
            # TODO: Make changes to avoid this overhead
            last_date = cls._get_last_date(security)
            if last_date is None:
                # No last_date has been set yet as the asset_base is still empty.
                pass
            else:
                last_date = pd.to_datetime(last_date)
                keep_index = pd.to_datetime(last_date) < series["date_stamp"]
                series = series.loc[keep_index]

            # Avoid empty series edge case
            if series.empty:
                continue
            # Create the security's series list of class instances and extend
            # onto the instances list
            instances = [
                cls(base_obj=security, **row) for index, row in series.iterrows()
            ]
            instances_list.extend(instances)

        # The bulk_save_objects does not work with inherited objects. Use
        # add_all instead.
        session.add_all(instances_list)

    @classmethod
    def to_data_frame(cls, session, asset_class):
        """Convert all instances to a single data table.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.

        Returns
        -------
        pandas.DataFrame
            A ``pandas.DataFrame`` with columns of the same name as all this
            class' constructor method arguments, with the exception that instead
            of a column named ``listed``, instead there shall be an ``isin``
            column with the ISIN number of the ``Listed`` instance.
        """

        # The goal is to substitute the `key_code_name` column for the
        # `Asset.id`.
        # Get Asset.key_code to Asset.id translation table
        key_code_id_table = asset_class.key_code_id_table(session)

        # Get all asset IDs for this asset class
        asset_ids = [asset_id for asset_id, in session.query(asset_class.id).all()]

        # Get only time-series instances that belong to assets of the specified
        # asset_class. We could have used a line `instance_table =
        # session.query(cls).all()` to get all instances of the class. Note:
        # SQLAlchemy's polymorphic inheritance automatically filters by
        # discriminator when querying cls (e.g., ListedEOD automatically gets
        # WHERE _discriminator='listed_eod'), but we add asset class filtering
        # as an additional safety measure to ensure data integrity and prevent
        # any potential issues if time series records somehow point to wrong
        # asset types.
        instances = session.query(cls).filter(cls._asset_id.in_(asset_ids)).all()
        if len(instances) == 0:
            # Return empty DataFrame with proper columns instead of raising exception.
            # This handles legitimate cases like new asset classes with no data yet,
            # or time periods where no data exists for the specified asset class.
            empty_df = pd.DataFrame()
            # If we have key_code_id_table, we can infer the proper columns
            if not key_code_id_table.empty:
                # Get the key_code_name column from the asset class
                key_code_name = asset_class.key_code_name
                # Add the key_code column and date columns
                empty_df[key_code_name] = pd.Series(dtype='object')
                for name in cls.date_column_names:
                    empty_df[name] = pd.Series(dtype='datetime64[ns]')
            return empty_df

        record_list = list()
        for instance in instances:
            # Get instance data dictionary and add the `Listed` ISIN number
            instance_dict = instance.to_dict()
            # Reference to the class primary key attribute Asset.id (or
            # polymorph child class)
            instance_dict["id"] = instance._asset_id
            record_list.append(instance_dict)
        instance_table = pd.DataFrame(record_list)

        # Join in the `id` column. Only for time series instances (left
        # join).
        data_table = instance_table.merge(key_code_id_table, on="id", how="left")
        data_table.drop(columns="id", inplace=True)

        # Convert date_stamp to pandas.Timestamp for all date columns. Note that
        # child classes may redefine the `date_column_names` list.
        for name in cls.date_column_names:
            data_table[name] = pd.to_datetime(data_table[name])

        return data_table

    @classmethod
    def update_all(cls, session, asset_class, get_method, asset_list):
        """Update/create the eod trade data of all the Listed instances.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all this class' constructor method arguments.
        asset_class : .asset.Asset (or child class)
            The ``Asset`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).
        asset_list : list of .Asset instances (or child instances)
            The list of assets which must be updated with time series data.

        No object shall be destroyed, only updated, or missing object created.

        """
        # The `financial_data` module's `get_method` individually considers the
        # `from_date` and `to_date` per asset based on the last time series date
        # of the relevant time series (this time series is determined by the
        # `get_method`).

        # Get all financial data from- the from_date till today.
        data_frame = get_method(asset_list)
        # Bulk add/update data.
        cls.from_data_frame(session, asset_class, data_frame)

    @classmethod
    def dump(cls, session, dumper: Dump, asset_class):
        """Dump all class instances and their time series data to disk.

        The data can be re-used to re-create all class instances and the time
        series data using the ``reuse`` method.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        dumper : .financial_data.Dump
            The financial data dumper.
        asset_class : .asset.Asset (or child class)
            The ``Asset`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).

        See also
        --------
        .reuse

        """
        dump_dict = dict()

        # A table item for  all instances of this class
        data_frame = cls.to_data_frame(session, asset_class)

        # Handle empty DataFrame case gracefully - still dump it but with a log message
        if data_frame.empty:
            logger.info(f"No time series data found for {cls._class_name()} with asset class {asset_class.__name__}. Creating empty dump file.")

        dump_dict[cls._class_name()] = data_frame
        # Serialize
        dumper.write(dump_dict)

    @classmethod
    def reuse(cls, session, dumper: Dump, asset_class):
        """Reuse dumped data as a database initialization resource.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        dumper : .financial_data.Dump
            The financial data dumper.
        asset_class : .asset.Asset (or child class)
            The ``Asset`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).

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
        # Uses dict data structures. See the docs.
        class_name = cls._class_name()
        data_frame_dict = dumper.read(name_list=[class_name])
        cls.from_data_frame(session, asset_class, data_frame_dict[class_name])


class SimpleEOD(TimeSeriesBase):
    """A single listed security's date-stamped EOD trade data.

    Parameters
    ----------
    base_obj : .asset.Base (or polymorph child class)
        The instance the time series data belongs to.
    date_stamp : datetime.date
        The end-of-day (EOD) data date stamp.
    price : float
        Price for the day.

    """

    __tablename__ = "simple_eod"
    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
    }

    id = Column(Integer, ForeignKey("time_series_base.id"), primary_key=True)
    """ Primary key."""

    _close = Column(Float, nullable=False)
    """float: Price for the day."""

    def __init__(self, base_obj, date_stamp, close):
        """Instance initialization."""
        super().__init__(base_obj, date_stamp)
        self._close = close

    def to_dict(self):
        """Convert all class price attributes to a dictionary.

        Returns
        -------
        dict
            date_stamp : datetime.date
            close : float (in currency units)
        """
        if self.base_obj.quote_units == "cents":
            return {
                "date_stamp": self.date_stamp,
                "close": self._close / 100.0,
            }
        else:
            return {
                "date_stamp": self.date_stamp,
                "close": self._close,
            }


class TradeEOD(SimpleEOD):
    """A single listed security's date-stamped EOD trade data.

    Parameters
    ----------
    base_obj : .asset.Base (or polymorph child class)
        The instance the time series data belongs to.
    date_stamp : datetime.date
        The end-of-day (EOD) data date stamp.
    open : float
        Open price for the day.
    close : float
        The EOD closing price for the day.
    high : float
        High price for the day.
    low : float
        Low price for the day.
    adjusted_close : float
        Adjusted close price for the day. The closing price is the raw price,
        which is just the cash value of the last transacted price before the
        market closes. The adjusted closing price factors in anything that might
        affect the stock price after the market closes.
    volume : float
        Number of shares traded in the day.

    """

    __tablename__ = "trade_eod"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    id = Column(Integer, ForeignKey("simple_eod.id"), primary_key=True)
    """ Primary key."""

    _open = Column(Float, nullable=False)
    """float: Open price for the day."""

    # Note that the `_close` attribute is inherited

    _high = Column(Float, nullable=False)
    """float: High price fpr the day."""

    _low = Column(Float, nullable=False)
    """float: Low price for the day."""

    _adjusted_close = Column(Float, nullable=False)
    """float: Adjusted close price for the day.

    The closing price is the raw price, which is just the cash value of the last
    transacted price before the market closes. The adjusted closing price
    factors in anything that might affect the stock price after the market
    closes.
    """

    _volume = Column(Integer, nullable=False)
    """float: Number of shares traded in the day."""

    def __init__(
        self, base_obj, date_stamp, open, close, high, low, adjusted_close, volume
    ):
        """Instance initialization."""
        super().__init__(
            base_obj,
            date_stamp,
            close=close,  # Convention that price=close price
        )
        self._open = open
        self._close = close
        self._high = high
        self._low = low
        self._adjusted_close = adjusted_close
        self._volume = volume

    def to_dict(self):
        """Convert all class price attributes to a dictionary.

        Returns
        -------
        dict
            date_stamp : datetime.date
            open: float (in currency units)
            close : float (in currency units)
            high : float (in currency units)
            low : float (in currency units)
            adjusted_close : float (in currency units)
            volume : int (number of units traded)
        """
        if self.base_obj.quote_units == "cents":
            return {
                "date_stamp": self.date_stamp,
                "open": self._open / 100.0,
                "close": self._close / 100.0,
                "high": self._high / 100.0,
                "low": self._low / 100.0,
                "adjusted_close": self._adjusted_close / 100.0,
                "volume": self._volume,
            }
        else:
            return {
                "date_stamp": self.date_stamp,
                "open": self._open,
                "close": self._close,
                "high": self._high,
                "low": self._low,
                "adjusted_close": self._adjusted_close,
                "volume": self._volume,
            }


class ListedEOD(TradeEOD):
    """A single listed security's date-stamped EOD trade data.

    Parameters
    ----------
    base_obj : .asset.Base (or polymorph child class)
        The instance the time series data belongs to. In this case
        ``.asset.Listed``.
    date_stamp : datetime.date
        The end-of-day (EOD) data date stamp.
    open : float
        Open price for the day.
    close : float
        The EOD closing price for the day.
    high : float
        High price for the day.
    low : float
        Low price for the day.
    adjusted_close : float
        Adjusted close price for the day. The closing price is the raw price,
        which is just the cash value of the last transacted price before the
        market closes. The adjusted closing price factors in anything that might
        affect the stock price after the market closes.
    volume : float
        Number of shares traded in the day.

    """

    __tablename__ = "listed_eod"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    id = Column(Integer, ForeignKey("trade_eod.id"), primary_key=True)
    """ Primary key."""

    # Foreign key giving ``Listed`` a EOD series capability. Note:
    # This doubles up on parent ``TimeSeriesBase._asset_id`` but is necessary
    # for the relationships with ``.asset.Share._series`` and
    # ``.asset.Listed._eod_series`` to work and avoids the
    # "SAWarning: relationship" warning for the relationship below.
    _listed_id = Column(Integer, ForeignKey("listed.id"), nullable=False)
    listed = relationship(
        "Listed", back_populates="_eod_series", foreign_keys=[_listed_id]
    )

    def __init__(
        self, base_obj, date_stamp, open, close, high, low, adjusted_close, volume
    ):
        """Instance initialization."""
        super().__init__(
            base_obj, date_stamp, open, close, high, low, adjusted_close, volume
        )
        self.listed = base_obj

    @classmethod
    def update_all(cls, session, get_method):
        """Update/create the eod trade data of all the Listed instances.

        Warning
        -------
        The Listed.time_series_last_date attribute (or child class attribute) is
        not updated by this method as it is the responsibility of the ``Listed``
        class and its child classes to manage that attribute.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all this class' constructor method arguments.

        No object shall be destroyed, only updated, or missing object created.

        """
        # TODO: Find a way to get the financial_data module to look for delisted
        # data. Then skipping de-listed securities below can be avoided

        # Get asset class from the asset relationship. This is to avoid an import from
        # `.asset` which would cause a circular import.
        asset_class = cls.listed.property.mapper.class_

        # For all de-listed securities skip data fetch and warn
        securities_delisted = (
            session.query(asset_class).filter(asset_class.status == "delisted").all()
        )
        for security in securities_delisted:
            logger.warning(
                f"Skipped {cls._class_name()} data fetch for "
                f"de-listed security {security.identity_code}."
            )

        # Get all actively listed Listed instances so we can fetch their
        # EOD trade data
        securities_list = (
            session.query(asset_class).filter(asset_class.status == "listed").all()
        )

        super().update_all(session, asset_class, get_method, securities_list)


class IndexEOD(TradeEOD):
    """A single index's date-stamped EOD trade data.

    Parameters
    ----------
    base_obj : .asset.Base (or polymorph child class)
        The instance the time series data belongs to. In this case
        ``.asset.Index``.
    date_stamp : datetime.date
        The end-of-day (EOD) data date stamp.
    open : float
        Open price for the day.
    close : float
        The EOD closing price for the day.
    high : float
        High price for the day.
    low : float
        Low price for the day.
    adjusted_close : float
        Adjusted close price for the day. The closing price is the raw price,
        which is just the cash value of the last transacted price before the
        market closes. The adjusted closing price factors in anything that might
        affect the stock price after the market closes.
    volume : float
        Number of shares traded in the day.

    """

    __tablename__ = "index_eod"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    id = Column(Integer, ForeignKey("trade_eod.id"), primary_key=True)
    """ Primary key."""

    # Foreign key giving ``Index`` a EOD series capability. Note:
    # This doubles up on parent ``TimeSeriesBase._asset_id`` but is necessary
    # for the relationships with ``.asset.Share._series`` and
    # ``.asset.Listed._eod_series`` to work and avoids the
    # "SAWarning: relationship" warning for the relationship below.
    _index_id = Column(Integer, ForeignKey("index.id"), nullable=False)
    index = relationship(
        "Index", back_populates="_eod_series", foreign_keys=[_index_id]
    )

    def __init__(
        self, base_obj, date_stamp, open, close, high, low, adjusted_close, volume
    ):
        """Instance initialization."""
        super().__init__(
            base_obj, date_stamp, open, close, high, low, adjusted_close, volume
        )
        self.index = base_obj

    @classmethod
    def update_all(cls, session, get_method):
        """Update/create the eod trade data of all the Listed instances.

        Warning
        -------
        The Listed.time_series_last_date attribute (or child class attribute) is
        not updated by this method as it is the responsibility of the ``Listed``
        class and its child classes to manage that attribute.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all this class' constructor method arguments.

        No object shall be destroyed, only updated, or missing object created.

        """
        # Get asset class from the asset relationship. This is to avoid an import from
        # `.asset` which would cause a circular import.
        asset_class = cls.index.property.mapper.class_

        # Get all actively listed Listed instances so we can fetch their
        # EOD trade data
        index_list = (
            # Do not use `asset_class.static is False` - `is` does NOT work. See
            # https://stackoverflow.com/questions/18998010/flake8-complains-on-boolean-comparison-in-filter-clause
            session.query(asset_class).filter(asset_class.static == False).all()  # noqa
        )

        super().update_all(session, asset_class, get_method, index_list)


class ForexEOD(TradeEOD):
    """A single forex date-stamped EOD trade data.

    Parameters
    ----------
    base_obj : .asset.Base (or polymorph child class)
        The instance the time series data belongs to. In this case
        ``.asset.Forex``.
    date_stamp : datetime.date
        The end-of-day (EOD) data date stamp.
    open : float
        Open price for the day.
    close : float
        The EOD closing price for the day.
    high : float
        High price fpr the day.
    low : float
        Low price for the day.
    adjusted_close : float
        Adjusted close price for the day. The closing price is the raw price,
        which is just the cash value of the last transacted price before the
        market closes. The adjusted closing price factors in anything that might
        affect the stock price after the market closes.
    volume : float
        Number of shares traded in the day.

    """

    __tablename__ = "forex_eod"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    id = Column(Integer, ForeignKey("trade_eod.id"), primary_key=True)
    """ Primary key."""

    # Foreign key giving ``Forex`` a EOD series capability. Note:
    # This doubles up on parent ``TimeSeriesBase._asset_id`` but is necessary
    # for the relationships with ``.asset.Share._series`` and
    # ``.asset.Forex._eod_series`` to work and avoids the
    # "SAWarning: relationship" warning for the relationship below.
    _forex_id = Column(Integer, ForeignKey("forex.id"), nullable=False)
    forex = relationship(
        "Forex", back_populates="_eod_series", foreign_keys=[_forex_id]
    )

    def __init__(
        self, base_obj, date_stamp, open, close, high, low, adjusted_close, volume
    ):
        """Instance initialization."""
        super().__init__(
            base_obj, date_stamp, open, close, high, low, adjusted_close, volume
        )
        self.forex = base_obj

    @classmethod
    def update_all(cls, session, get_method):
        """Update/create the eod trade data of all the Listed instances.

        Warning
        -------
        The Listed.time_series_last_date attribute (or child class attribute) is
        not updated by this method as it is the responsibility of the ``Listed``
        class and its child classes to manage that attribute.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all this class' constructor method arguments.

        No object shall be destroyed, only updated, or missing object created.

        """
        # Get asset class from the asset relationship. This is to avoid an import from
        # `.asset` which would cause a circular import.
        asset_class = cls.forex.property.mapper.class_

        # Get all actively listed Listed instances so we can fetch their
        # EOD trade data
        forex_list = session.query(asset_class).all()

        super().update_all(session, asset_class, get_method, forex_list)


class Dividend(TimeSeriesBase):
    """A single listed security's date-stamped EOD trade data.

    Parameters
    ----------
    base_obj : .asset.Base (or polymorph child class)
        The instance the time series data belongs to. In this case
        ``.asset.ListedEquity``.
    date_stamp : datetime.date
        The end-of-day (EOD) data date stamp.
    currency : str(3)
        ISO 4217 3-letter currency codes.
    declaration_date : datetime.date
        The date the dividend was declared.
    payment_date : datetime.date
        The date the dividend was paid across.
    period : str ('Quarterly', 'semi-annually' or 'annually')
        The period of payment, Quarterly, semi-annually or annually
    record_date : datetime.date
        The date the dividend was recorded.
    unadjusted_value : float
        The unadjusted value of the dividend in indicated currency.
    adjusted_value : float
        The adjusted value of the dividend in indicated currency.

    """

    __tablename__ = "dividend"
    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
    }

    id = Column(Integer, ForeignKey("time_series_base.id"), primary_key=True)
    """ Primary key."""
    # NOTE: Inherited
    # backrefs to the polymorphic ListedEquity

    # Foreign key giving ``ListedEquity`` a Dividend series capability. Note:
    # This doubles up on parent ``TimeSeriesBase._asset_id`` but is necessary
    # for the relationships with ``.asset.Asset._series`` and
    # ``.asset.ListedEquity._dividend_series`` to work and avoids the
    # "SAWarning: relationship" warning for the relationship below.
    _listed_equity_id = Column(Integer, ForeignKey("listed_equity.id"), nullable=False)
    listed_equity = relationship(
        "ListedEquity",
        back_populates="_dividend_series",
        foreign_keys=[_listed_equity_id],
    )

    # FIXME: Cannot have NULL here but some dividends are triggering tis
    currency = Column(String, nullable=True)
    """str: 3-Letter currency symbol for the dividend currency. """

    declaration_date = Column(Date, nullable=True)
    """datetime: The date the dividend was declared. """

    payment_date = Column(Date, nullable=True)
    """datetime: The date the dividend was paid across. """

    period = Column(String, nullable=True)
    """str: The period of payment, Quarterly, semi-annually or annually."""

    record_date = Column(Date, nullable=True)
    """datetime: The date the dividend was recorded. """

    _unadjusted_value = Column(Float, nullable=True)
    """float: The unadjusted value of the dividend in indicated currency. """

    _adjusted_value = Column(Float, nullable=True)
    """float: The adjusted value of the dividend in indicated currency. """

    date_column_names = [
        "date_stamp",
        "declaration_date",
        "payment_date",
        "record_date",
    ]
    """list: Columns that must be exported externally as pandas.Timestamp."""

    def __init__(
        self,
        base_obj,
        date_stamp,
        currency,
        declaration_date,
        payment_date,
        period,
        record_date,
        unadjusted_value,
        adjusted_value,
        **kwargs,
    ):
        """Instance initialization."""
        super().__init__(base_obj, date_stamp)
        self.listed_equity = base_obj
        self.currency = currency
        self.declaration_date = declaration_date
        self.payment_date = payment_date
        self.period = period
        self.record_date = record_date
        self._unadjusted_value = unadjusted_value
        self._adjusted_value = adjusted_value

    @classmethod
    def _get_last_date(cls, security):
        """Get the time series last date maintained by the security."""
        return security.get_last_dividend_date()

    def to_dict(self):
        """Convert all class dividend attributes to a dictionary.

        Returns
        -------
        dict
            date_stamp : datetime.date
            currency : str (ISO 4217 3-letter currency code)
            declaration_date : str (yyyy-mm-dd)
            payment_date : str (yyyy-mm-dd)
            period : str ('Quarterly', 'semi-annually' or 'annually')
            record_date : str (yyyy-mm-dd)
            unadjusted_value : float (in currency units)
            adjusted_value : float (in currency units)
        """
        if self.base_obj.quote_units == "cents":
            return {
                "date_stamp": self.date_stamp,
                "currency": self.currency,
                "declaration_date": self.declaration_date,
                "payment_date": self.payment_date,
                "period": self.period,
                "record_date": self.record_date,
                "unadjusted_value": self._unadjusted_value / 100.0,
                "adjusted_value": self._adjusted_value / 100.0,
            }
        else:
            return {
                "date_stamp": self.date_stamp,
                "currency": self.currency,
                "declaration_date": self.declaration_date,
                "payment_date": self.payment_date,
                "period": self.period,
                "record_date": self.record_date,
                "unadjusted_value": self._unadjusted_value,
                "adjusted_value": self._adjusted_value,
            }

    @classmethod
    def update_all(cls, session, get_method):
        """Update/create the eod trade data of all the Listed instances.

        Warning
        -------
        The Listed.time_series_last_date attribute (or child class attribute) is
        not updated by this method as it is the responsibility of the ``Listed``
        class and its child classes to manage that attribute.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all this class' constructor method arguments.

        No object shall be destroyed, only updated, or missing object created.

        """
        # TODO: Find a way to get the financial_data module to look for delisted
        # data. Then skipping de-listed securities below can be avoided

        # Get asset class from the asset relationship. This is to avoid an import from
        # `.asset` which would cause a circular import.
        asset_class = cls.listed_equity.property.mapper.class_

        # Skip data fetch and warn for all de-listed securities
        securities_delisted = (
            session.query(asset_class).filter(asset_class.status == "delisted").all()
        )
        for security in securities_delisted:
            logger.warning(
                f"Skipped {cls._class_name()} data fetch for "
                f"de-listed security {security.identity_code}."
            )

        # Get all actively listed Listed instances so we can fetch their
        # EOD trade data
        securities_list = (
            session.query(asset_class).filter(asset_class.status == "listed").all()
        )

        super().update_all(session, asset_class, get_method, securities_list)
