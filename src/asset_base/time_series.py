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

from abc import abstractmethod
from datetime import date
import datetime
import sys
import numpy as np
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
    """ Common time-series relationships and capabilities.

    Note
    ----
    This is a base class not to be instantiated directly.

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

    _id = Column(Integer, primary_key=True, autoincrement=True)
    """int: Primary key."""

    __table_args__ = (UniqueConstraint("_discriminator", "_asset_id", "date_stamp"),)

    # Foreign key giving the ``Asset`` class a generic time series capability
    _asset_id = Column(Integer, ForeignKey("asset_base._id"), nullable=False)
    _base_obj = relationship(
        "AssetBase", back_populates="_series", foreign_keys=[_asset_id])

    date_stamp = Column(Date, nullable=False)
    # TODO: Consider making this part of the primary keys
    """datetime: EOD date."""

    _date_column_names = ["date_stamp"]
    """list: Columns that must be exported externally as pandas.Timestamp."""

    def __init__(self, base_obj, date_stamp):
        """Instance initialization."""
        # Type check date_stamp - must be datetime.date but not pandas.Timestamp
        if not isinstance(date_stamp, datetime.date) or isinstance(date_stamp, pd.Timestamp):
            raise TypeError(
                f"The `date_stamp` must be a datetime.date instance, not "
                f"{type(date_stamp)}. Use .date() to convert pandas.Timestamp.")

        # Validate base_obj type if ASSET_CLASS is defined
        if hasattr(self.__class__, 'ASSET_CLASS') and self.__class__.ASSET_CLASS is not None:
            if not isinstance(base_obj, self.__class__.ASSET_CLASS):
                raise TypeError(
                    f"{self.__class__.__name__} requires base_obj to be an instance of "
                    f"{self.__class__.ASSET_CLASS.__name__}, got {type(base_obj).__name__}")

        self._base_obj = base_obj
        self.date_stamp = date_stamp

    @abstractmethod
    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        pass

    @abstractmethod
    def __repr__(self):
        """Return the official string output."""
        pass

    @abstractmethod
    def to_dict(self):
        """Convert all instance time-series public attributes to a dictionary."""
        pass

    @classmethod
    @abstractmethod
    def update_all(cls, session, get_method):
        """Update/create all time-series for the list of asset instances.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all this class' constructor method arguments.
        """
        pass

    @classmethod
    def from_data_frame(cls, session, asset_class, data_frame):
        # FIXME: Use overloaded method pointer class attributes instead of these arguments
        """Insert instances into the database from ``pandas.DataFrame`` rows.

        This method uses the SqlAlchemy session ``add_all`` method to add or
        updates all the ``asset.Asset`` child class' time series.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        asset_class : .asset.AssetBase or child class
            The ``Base`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).
        data_frame : pandas.DataFrame
            A ``pandas.DataFrame`` with columns with names identical to the
            ``TimeSeriesBase`` child class' constructor' method arguments, with
            the exception that there must be no would-be column named
            `base_obj`. Instead there must be a column with the name given by
            the `asset_class.KEY_CODE_LABEL` class attribute. This
            `asset_class.KEY_CODE_LABEL` is a string giving the name of the
            column that contains values that allow for the identification of the
            `asset_class` instance that the time-series data row belongs to. For
            example, if the `asset_class` is ``asset.Listed``, then the
            `KEY_CODE_LABEL` and therefore column name would be `isin` with
            listed security ISIN numbers as rows. If for example the
            `asset_class` is ``asset.Cash``, then the `KEY_CODE_LABEL` and
            therefore column name would be `asset_currency`. This column must be
            populated with the `<asset_class>.key_code` respective class
            attribute value. This text string based approach is more convenient
            for formatting data identity when fetched from financial data API
            services.
        """

        # Check for zero rows of data
        if data_frame.empty:
            # No data so return
            return

        data_table = data_frame

        # Check for non-uniqueness by date_stamp and KEY_CODE_LABEL
        if data_table.duplicated(subset=["date_stamp", asset_class.KEY_CODE_LABEL]).any():
            raise ValueError(
                f"Duplicate rows found in {cls.__name__} data for "
                f"date_stamp and {asset_class.KEY_CODE_LABEL}. Duplicates will be removed."
            )

        # Guarantee date ranking of the data
        data_table.sort_values(by="date_stamp")

        # Replace pesky pd.NaT with None. Else with the SQLite backend
        # SqlAlchemy DateTime columns throw `(builtins.ValueError) cannot
        # convert float NaN to integer` SQLite, which doesn't have a native
        # DateTime type, cannot convert a float NaN to an integer when trying to
        # store the datetime value.
        data_table.replace({pd.NaT: None}, inplace=True)

        # Get Asset.key_code to Asset._id translation table
        key_code_id_table = asset_class.key_code_id_table(session)
        # Join to create a new extended instance_table with the security column.
        # Only for time series instances (left join).
        # FIXME: Check if there are securities in data_table that are not in key_code_id_table
        data_table = data_table.merge(key_code_id_table, on=asset_class.KEY_CODE_LABEL, how="left")
        data_table.drop(columns=asset_class.KEY_CODE_LABEL, inplace=True)
        data_table.set_index(["id", "date_stamp"], inplace=True, drop=True)

        # With certain date ranges in a data fetch there may be no new data
        if data_table.empty:
            # Nothing to process so just return
            return
        # Fetch the relevant .asset.Asset or polymorph instances
        asset_id_list = data_table.index.to_frame(index=False).id.drop_duplicates().to_list()
        asset_list = (
            session.query(asset_class).filter(asset_class._id.in_(asset_id_list)).all()
        )
        # Add data to each security's time series' asset_class
        for asset in asset_list:
            # Get the security's time series.
            time_series_df = data_table.loc[asset._id]
            # Sort by ascending date_stamp. Use a copy (inplace=False) to avoid
            # a SettingWithCopyWarning
            time_series_df.sort_index(inplace=True)
            # Reset date_stamp index making it a column
            time_series_df.reset_index(inplace=True)

            # Bulk upsert approach: query existing records first, then bulk operations
            existing_records = session.query(cls).filter(
                cls._asset_id == asset._id,
                cls.date_stamp.in_(time_series_df["date_stamp"].tolist())
            ).all()

            # Create lookup of existing records by date_stamp
            existing_by_date = {record.date_stamp: record for record in existing_records}

            new_instances = []
            for _, row in time_series_df.iterrows():
                # Convert to datetime.date - handle pandas.Timestamp and datetime.date explicitly
                raw_date = row["date_stamp"]
                if isinstance(raw_date, pd.Timestamp):
                    date_stamp = raw_date.date()
                elif isinstance(raw_date, datetime.date):
                    date_stamp = raw_date
                else:
                    raise TypeError(f"Unsupported date type {type(raw_date)} for date_stamp")

                # Check if record exists - we're updating or inserting
                # accordingly (only public attrs)
                if date_stamp in existing_by_date:
                    # Update existing record - only modify public attributes
                    existing_record = existing_by_date[date_stamp]
                    for key, value in row.items():
                        if key != "date_stamp":  # Don't update the key field
                            # Only update if it's a public attribute (no leading underscore)
                            if hasattr(existing_record, key) and not key.startswith('_'):
                                setattr(existing_record, key, value)
                else:
                    # Create new instance for bulk insert
                    # Convert pandas row to dict and fix date_stamp
                    row_dict = row.to_dict()
                    row_dict["date_stamp"] = date_stamp  # Must be type datetime.date
                    new_instances.append(cls(base_obj=asset, **row_dict))

            # Bulk insert only new records
            if new_instances:
                session.add_all(new_instances)
            pass

    @classmethod
    def to_data_frame(cls, session, asset_class):
        """Return a ``pandas.DataFrame`` with a row for each class instance.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        asset_class : .asset.Asset (or polymorph class)
            The ``Asset`` or polymorph class that is composed of the time-series
            data.

        Returns
        -------
        pandas.DataFrame
            A ``pandas.DataFrame`` with columns of the same name as all this
            class' constructor method arguments, with the exception that instead
            of a column named ``listed``, instead there shall be an ``isin``
            column with the ISIN number of the ``Listed`` instance.
        """

        # Generate a table translating key_code to asset_class._id. The key_code
        # column label shall be the asset_class.KEY_CODE_LABEL class
        # attribute value.
        key_code_id_table = asset_class.key_code_id_table(session)

        # Get all asset IDs for this asset class
        asset_ids = [asset_id for asset_id, in session.query(asset_class._id).all()]

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
                # Get the KEY_CODE_LABEL column from the asset class and add the
                # key_code column and date columns
                empty_df[asset_class.KEY_CODE_LABEL] = pd.Series(dtype='object')
                for name in cls._date_column_names:
                    empty_df[name] = pd.Series(dtype='datetime64[ns]')
            return empty_df

        record_list = list()
        for instance in instances:
            # Get instance data dictionary and add the `Listed` ISIN number
            instance_dict = instance.to_dict()
            # Reference to the class primary key attribute Asset._id (or
            # polymorph child class)
            instance_dict["id"] = instance._asset_id
            record_list.append(instance_dict)
        instance_table = pd.DataFrame(record_list)

        # SQLAlchemy with the SQLite backend can only store `None` for DateTime
        # columns. So we must convert any `None` back to `np.nan` for proper
        # pandas DataFrame representation.
        instance_table.replace({None: np.nan}, inplace=True)

        # Join in the `id` column. Only for time series instances (left
        # join).
        data_table = instance_table.merge(key_code_id_table, on="id", how="left")
        data_table.drop(columns="id", inplace=True)

        # Convert date_stamp to pandas.Timestamp for all date columns. Note that
        # child classes may redefine the `date_column_names` list.
        for name in cls._date_column_names:
            data_table[name] = pd.to_datetime(data_table[name])

        return data_table

    @classmethod
    def dump(cls, session, dumper: Dump, asset_class_cls):
        """Dump all class instances and their time series data to disk.

        The data can be re-used to re-create all class instances and the time
        series data using the ``reuse`` method.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        dumper : .financial_data.Dump
            The financial data dumper.
        asset_class_cls : .asset.Asset (or child class) class reference
            The ``Asset`` class reference which which when instantiated could be
            composed of this time-series data.

        See also
        --------
        .reuse

        """
        dump_dict = dict()

        # A table item for  all instances of this class
        data_frame = cls.to_data_frame(session, asset_class_cls)

        # Handle empty DataFrame case gracefully - still dump it but with a log message
        if data_frame.empty:
            logger.warning(
                f"No time series data found for {cls.__name__}. "
                "Creating empty dump file.")

        dump_dict[cls.__name__] = data_frame
        # Serialize
        dumper.write(dump_dict)

    @classmethod
    def reuse(cls, session, dumper: Dump, asset_class_cls):
        """Reuse dumped data as a database initialization resource.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        dumper : .financial_data.Dump
            The financial data dumper.
        asset_class_cls : .asset.Asset (or child class) class reference
            The ``Asset`` class reference which which when instantiated could be
            composed of this time-series data.

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
        class_name = cls.__name__
        # Restricted to only one item at a time
        data_frame_dict = dumper.read([class_name])

        # For reuse operations, ensure we delete any existing records first
        # to prevent conflicts during bulk insert
        existing_count = session.query(cls).count()
        if existing_count > 0:
            session.query(cls).delete()
            session.flush()

        cls.from_data_frame(session, asset_class_cls, data_frame_dict[class_name])


class EODBase(TimeSeriesBase):
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

    _id = Column(Integer, ForeignKey("time_series_base._id"), primary_key=True)
    """ Primary key."""

    price = Column(Float, nullable=False)
    """float: Price for the day."""

    def __init__(self, base_obj, date_stamp, price):
        """Instance initialization."""
        super().__init__(base_obj, date_stamp)
        self.price = price

    def __str__(self):
        """Return the informal string output."""
        return f"EODBase({self._base_obj.identity_code}, {self.date_stamp}, price={self.price})"

    def __repr__(self):
        """Return the official string output."""
        return (
            f"EODBase(base_obj={self._base_obj!r}, date_stamp={self.date_stamp!r}, "
            f"price={self.price})"
        )

    def to_dict(self):
        """Convert all class price attributes to a dictionary.

        Returns
        -------
        dict
            date_stamp : datetime.date
            close : float (in currency units)
        """
        if self._base_obj.quote_units == "cents":
            return {
                "date_stamp": self.date_stamp,
                "price": self.price / 100.0,
            }
        else:
            return {
                "date_stamp": self.date_stamp,
                "price": self.price,
            }


class TradeEOD(EODBase):
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

    Note
    ----
    The `close` parameter shall be use to set the inherited ``EODBase.price`` as
    a convention.

    """

    __tablename__ = "trade_eod"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    _id = Column(Integer, ForeignKey("simple_eod._id"), primary_key=True)
    """ Primary key."""

    open = Column(Float, nullable=False)
    """float: Open price for the day."""

    close = Column(Float, nullable=False)
    """float: Close price for the day."""

    high = Column(Float, nullable=False)
    """float: High price fpr the day."""

    low = Column(Float, nullable=False)
    """float: Low price for the day."""

    adjusted_close = Column(Float, nullable=False)
    """float: Adjusted close price for the day.

    The closing price is the raw price, which is just the cash value of the last
    transacted price before the market closes. The adjusted closing price
    factors in anything that might affect the stock price after the market
    closes.
    """

    volume = Column(Integer, nullable=False)
    """float: Number of shares traded in the day."""

    def __init__(
        self, base_obj, date_stamp, open, close, high, low, adjusted_close, volume
    ):
        """Instance initialization."""
        super().__init__(
            base_obj,
            date_stamp,
            price=close,  # Convention that price=close price
        )
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.adjusted_close = adjusted_close
        self.volume = volume

    def __str__(self):
        """Return the informal string output."""
        return f"TradeEOD({self._base_obj.identity_code}, {self.date_stamp}, close={self.close})"

    def __repr__(self):
        """Return the official string output."""
        return (
            f"TradeEOD(base_obj={self._base_obj!r}, date_stamp={self.date_stamp!r}, "
            f"open={self.open}, close={self.close}, high={self.high}, low={self.low}, "
            f"adjusted_close={self.adjusted_close}, volume={self.volume})"
        )

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

        Note
        ----
        The inherited ``EODBase.price`` attribute is not included in the output
        as it is by convention identical to the `close` value.

        """
        if self._base_obj.quote_units == "cents":
            return {
                "date_stamp": self.date_stamp,
                "open": self.open / 100.0,
                "close": self.close / 100.0,
                "high": self.high / 100.0,
                "low": self.low / 100.0,
                "adjusted_close": self.adjusted_close / 100.0,
                "volume": self.volume,
            }
        else:
            return {
                "date_stamp": self.date_stamp,
                "open": self.open,
                "close": self.close,
                "high": self.high,
                "low": self.low,
                "adjusted_close": self.adjusted_close,
                "volume": self.volume,
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

    _id = Column(Integer, ForeignKey("trade_eod._id"), primary_key=True)
    """ Primary key."""

    # Define which asset class this time series belongs to
    ASSET_CLASS = None  # Will be set after Listed is imported to avoid circular import

    def __init__(
        self, base_obj, date_stamp, open, close, high, low, adjusted_close, volume
    ):
        """Instance initialization."""
        super().__init__(
            base_obj, date_stamp, open, close, high, low, adjusted_close, volume
        )

    def __str__(self):
        """Return the informal string output."""
        return f"ListedEOD({self._base_obj.isin}, {self.date_stamp}, close={self.close})"

    def __repr__(self):
        """Return the official string output."""
        return (
            f"ListedEOD(base_obj={self._base_obj!r}, date_stamp={self.date_stamp!r}, "
            f"open={self.open}, close={self.close}, high={self.high}, low={self.low}, "
            f"adjusted_close={self.adjusted_close}, volume={self.volume})"
        )

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

        Note
        ----
        No object shall be destroyed, only updated or missing object created.

        Note
        ----
        This method only updates actively listed securities.

        """
        # Import here to avoid circular import
        from .asset import Listed

        # Get asset class
        asset_class = Listed

        # Get only all actively listed Listed instances so we can fetch their
        # EOD trade data
        securities_list = (
            session.query(asset_class).filter(asset_class.status == "listed").all()
        )

        # Get all financial data
        data_frame = get_method(securities_list)
        # Bulk add/update data.
        cls.from_data_frame(session, asset_class, data_frame)


class ListedEquityEOD(ListedEOD):
    """A single listed equity's date-stamped EOD trade data.

    This is a specialized version of ListedEOD for equity securities specifically.
    It inherits all functionality from ListedEOD but provides a distinct table
    and identity for equity-specific time series data.

    Parameters
    ----------
    base_obj : .asset.ListedEquity (or child class)
        The instance the time series data belongs to. In this case
        ``.asset.ListedEquity``.
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

    __tablename__ = "listed_equity_eod"
    __mapper_args__ = {"polymorphic_identity": __tablename__}

    _id = Column(Integer, ForeignKey("listed_eod._id"), primary_key=True)
    """ Primary key."""

    # Define which asset class this time series belongs to
    ASSET_CLASS = None  # Will be set after ListedEquity is imported to avoid circular import

    def __init__(
        self, base_obj, date_stamp, open, close, high, low, adjusted_close, volume
    ):
        """Instance initialization."""
        super().__init__(
            base_obj, date_stamp, open, close, high, low, adjusted_close, volume
        )

    def __str__(self):
        """Return the informal string output."""
        return f"ListedEquityEOD({self._base_obj.isin}, {self.date_stamp}, close={self.close})"

    def __repr__(self):
        """Return the official string output."""
        return (
            f"ListedEquityEOD(base_obj={self._base_obj!r}, date_stamp={self.date_stamp!r}, "
            f"open={self.open}, close={self.close}, high={self.high}, low={self.low}, "
            f"adjusted_close={self.adjusted_close}, volume={self.volume})"
        )

    @classmethod
    def update_all(cls, session, get_method):
        """Update/create the eod trade data of all the ListedEquity instances.

        Warning
        -------
        The ListedEquity.time_series_last_date attribute (or child class attribute) is
        not updated by this method as it is the responsibility of the ``ListedEquity``
        class and its child classes to manage that attribute.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_method : financial_data module class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all this class' constructor method arguments.

        Note
        ----
        No object shall be destroyed, only updated or missing object created.

        Note
        ----
        This method only updates actively listed equity securities.

        """
        # Import here to avoid circular import
        from .asset import ListedEquity

        # Get asset class
        asset_class = ListedEquity

        # Get only all actively listed ListedEquity instances so we can fetch their
        # EOD trade data
        securities_list = (
            session.query(asset_class).filter(asset_class.status == "listed").all()
        )

        # Get all financial data
        data_frame = get_method(securities_list)
        # Bulk add/update data.
        cls.from_data_frame(session, asset_class, data_frame)


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

    _id = Column(Integer, ForeignKey("trade_eod._id"), primary_key=True)
    """ Primary key."""

    # Define which asset class this time series belongs to
    ASSET_CLASS = None  # Will be set after Index is imported to avoid circular import

    def __init__(
        self, base_obj, date_stamp, open, close, high, low, adjusted_close, volume
    ):
        """Instance initialization."""
        super().__init__(
            base_obj, date_stamp, open, close, high, low, adjusted_close, volume
        )

    def to_dict(self):
        """Convert all class price attributes to a dictionary.

        Returns
        -------
        dict
            date_stamp : datetime.date
            open: float
            close : float
            high : float
            low : float
            adjusted_close : float
            volume : int (number of units traded)

        Note
        ----
        Index objects don't have quote_units attribute, so values are always
        returned as-is without conversion.

        """
        return {
            "date_stamp": self.date_stamp,
            "open": self.open,
            "close": self.close,
            "high": self.high,
            "low": self.low,
            "adjusted_close": self.adjusted_close,
            "volume": self.volume,
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

        Note
        ----
        No object shall be destroyed, only updated or missing object created.

        """
        # Get asset class - use the ASSET_CLASS attribute set during initialization
        asset_class = cls.ASSET_CLASS

        # Get all actively listed Listed instances so we can fetch their
        # EOD trade data
        index_list = (
            session.query(asset_class).filter(asset_class.static == False).all()  # noqa
        )

        super().update_all(session, asset_class, index_list, get_method)


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

    _id = Column(Integer, ForeignKey("trade_eod._id"), primary_key=True)
    """ Primary key."""

    # Define which asset class this time series belongs to
    ASSET_CLASS = None  # Will be set after Forex is imported to avoid circular import

    def __init__(
        self, base_obj, date_stamp, open, close, high, low, adjusted_close, volume
    ):
        """Instance initialization."""
        super().__init__(
            base_obj, date_stamp, open, close, high, low, adjusted_close, volume
        )

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

        Note
        ----
        No object shall be destroyed, only updated or missing object created.

        """
        # Get asset class - use the ASSET_CLASS attribute set during initialization
        asset_class = cls.ASSET_CLASS

        # Get all actively listed Listed instances so we can fetch their
        # EOD trade data
        forex_list = session.query(asset_class).all()

        super().update_all(session, asset_class, forex_list, get_method)


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

    _id = Column(Integer, ForeignKey("time_series_base._id"), primary_key=True)
    """ Primary key."""

    # Define which asset class this time series belongs to
    ASSET_CLASS = None  # Will be set after ListedEquity is imported to avoid circular import

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

    unadjusted_value = Column(Float, nullable=True)
    """float: The unadjusted value of the dividend in indicated currency. """

    adjusted_value = Column(Float, nullable=True)
    """float: The adjusted value of the dividend in indicated currency. """

    _date_column_names = [
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
    ):
        """Instance initialization."""
        super().__init__(base_obj, date_stamp)
        self.currency = currency
        self.declaration_date = declaration_date
        self.payment_date = payment_date
        self.period = period
        self.record_date = record_date
        self.unadjusted_value = unadjusted_value
        self.adjusted_value = adjusted_value

    def __str__(self):
        """Return the informal string output."""
        return f"Dividend({self._base_obj.isin}, {self.date_stamp}, value={self.adjusted_value})"

    def __repr__(self):
        """Return the official string output."""
        return (
            f"Dividend(base_obj={self._base_obj!r}, date_stamp={self.date_stamp!r}, "
            f"currency={self.currency!r}, declaration_date={self.declaration_date!r}, "
            f"payment_date={self.payment_date!r}, period={self.period!r}, "
            f"record_date={self.record_date!r}, unadjusted_value={self.unadjusted_value}, "
            f"adjusted_value={self.adjusted_value})"
        )

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
        if self._base_obj.quote_units == "cents":
            return {
                "date_stamp": self.date_stamp,
                "currency": self.currency,
                "declaration_date": self.declaration_date,
                "payment_date": self.payment_date,
                "period": self.period,
                "record_date": self.record_date,
                "unadjusted_value": self.unadjusted_value / 100.0,
                "adjusted_value": self.adjusted_value / 100.0,
            }
        else:
            return {
                "date_stamp": self.date_stamp,
                "currency": self.currency,
                "declaration_date": self.declaration_date,
                "payment_date": self.payment_date,
                "period": self.period,
                "record_date": self.record_date,
                "unadjusted_value": self.unadjusted_value,
                "adjusted_value": self.adjusted_value,
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

        Note
        ----
        No object shall be destroyed, only updated or missing object created.

        Note
        ----
        This method only updates actively listed securities.

        """
        # Get asset class - use the ASSET_CLASS attribute set during initialization
        asset_class = cls.ASSET_CLASS

        # Get all actively listed Listed instances so we can fetch their data
        securities_list = (
            session.query(asset_class).filter(asset_class.status == "listed").all()
        )

        super().update_all(session, asset_class, securities_list, get_method)


class Split(TimeSeriesBase):
    """A single listed security's date-stamped split data.

    Parameters
    ----------
    base_obj : .asset.Base (or polymorph child class)
        The instance the time series data belongs to. In this case
        ``.asset.ListedEquity``.
    date_stamp : datetime.date
        The end-of-day (EOD) data date stamp.
    numerator : float
        The numerator of the split ratio, e.g., 2 for a 2-for-1 split.
    denominator : float
        The denominator of the split ratio, e.g., 1 for a 2-for-1 split.

    """

    __tablename__ = "split"
    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
    }

    _id = Column(Integer, ForeignKey("time_series_base._id"), primary_key=True)
    """ Primary key."""

    # Define which asset class this time series belongs to
    ASSET_CLASS = None  # Will be set after ListedEquity is imported to avoid circular import

    numerator = Column(Float, nullable=False)
    """float: The numerator of the split ratio, e.g., 2 for a 2-for-1 split."""
    denominator = Column(Float, nullable=False)
    """float: The denominator of the split ratio, e.g., 1 for a 2-for-1 split."""

    def __init__(self, base_obj, date_stamp, numerator, denominator):
        """Instance initialization."""
        super().__init__(base_obj, date_stamp)
        self.numerator = numerator
        self.denominator = denominator

    def __str__(self):
        """Return the informal string output."""
        return f"Split({self._base_obj.isin}, {self.date_stamp}, {self.numerator}:{self.denominator})"

    def __repr__(self):
        """Return the official string output."""
        return (
            f"Split(base_obj={self._base_obj!r}, date_stamp={self.date_stamp!r}, "
            f"numerator={self.numerator}, denominator={self.denominator})"
        )

    @classmethod
    def _get_last_date(cls, security):
        """Get the time series last date maintained by the security."""
        return security.get_last_split_date()

    def to_dict(self):
        """Convert all class split attributes to a dictionary.

        Returns
        -------
        dict
            date_stamp : datetime.date
            numerator : float
            denominator : float
            ratio : float
        """
        return {
            "date_stamp": self.date_stamp,
            "numerator": self.numerator,
            "denominator": self.denominator,
            # TODO: Add unittest for this new field
            "ratio": self.numerator / self.denominator,
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

        Note
        ----
        No object shall be destroyed, only updated or missing object created.

        Note
        ----
        This method only updates actively listed securities.

        """
        # TODO: Find a way to get the financial_data module to look for delisted
        # data. Then skipping de-listed securities below can be avoided

        # Get asset class - use the ASSET_CLASS attribute set during initialization
        asset_class = cls.ASSET_CLASS

        # Get all actively listed Listed instances so we can fetch their data
        securities_list = (
            session.query(asset_class).filter(asset_class.status == "listed").all()
        )

        super().update_all(session, asset_class, securities_list, get_method)


# Initialize ASSET_CLASS attributes after all classes are defined
# This must be called after asset module is imported to avoid circular imports
def _initialize_asset_class_references():
    """Initialize ASSET_CLASS attributes for time series classes.

    This function should be called once after the asset module is fully loaded
    to set up the bidirectional type references between asset and time series classes.
    This avoids circular import issues while maintaining type safety.
    """
    from .asset import Listed, ListedEquity, Index, Forex

    # Set asset class references for EOD time series
    ListedEOD.ASSET_CLASS = Listed
    ListedEquityEOD.ASSET_CLASS = ListedEquity
    IndexEOD.ASSET_CLASS = Index
    ForexEOD.ASSET_CLASS = Forex

    # Set asset class references for dividend and split time series
    Dividend.ASSET_CLASS = ListedEquity
    Split.ASSET_CLASS = ListedEquity
