#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Support for time series data.
"""
# Allows  in type hints to use class names instead of class name strings
from __future__ import annotations
# Used to avoid ImportError (most likely due to a circular import)
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from asset_base.asset import Asset

import sys
import datetime
import pandas as pd

from sqlalchemy import Float, Integer, String, Date
from sqlalchemy import MetaData, Column, ForeignKey
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm.exc import NoResultFound

from asset_base.financial_data import Dump

from asset_base.exceptions import FactoryError

# Import the common declarative base
from asset_base.common import Base

# Get module-named logger.
import logging
logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Pull in the meta data
metadata = MetaData()


class TimeSeriesMeta(Base):
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

    __tablename__ = 'time_series_meta'

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
        return f'TimeSeriesMeta name={self.name}, value={self.value}'

    def __repr__(self):
        """Return the official string output."""
        return f'TimeSeriesMeta(name={self.name}, value={self.value})'

    @classmethod
    def set_value(cls, session, name, value):
        """Set a named meta data value"""
        try:
            # Try to get the existing instance
            obj = session.query(TimeSeriesMeta).filter(
                TimeSeriesMeta.name == name).one()
        except NoResultFound:
            # Create and add the new instance
            obj = TimeSeriesMeta(name, value)
            session.add(obj)
        else:
            # Update existing instance
            obj.value = value

    @classmethod
    def get_value(cls, session, name):
        """Get a named meta data value"""
        # Try to get the existing instance
        obj = session.query(TimeSeriesMeta).filter(
            TimeSeriesMeta.name == name).one()

        return obj.value

    @classmethod
    def set_last_date(
            cls, session,
            asset_class: Asset, ts_class: TimeSeriesBase, date: datetime.date):
        """Set the last date for an ``.asset`` class."""
        asset_class_name = asset_class._class_name
        ts_class_name = ts_class._class_name
        cls.set_value(
            session,
            name=f'{asset_class_name}.{ts_class_name}.last_date',
            value=date.strftime('%Y-%m-%d'))

    @classmethod
    def get_last_date(
            cls, session, asset_class: Asset, ts_class: TimeSeriesBase
    ) -> datetime.date:
        """Get the last date for an ``.asset`` class."""
        asset_class_name = asset_class._class_name
        ts_class_name = ts_class._class_name
        try:
            value = cls.get_value(
                session,
                name=f'{asset_class_name}.{ts_class_name}.last_date',
            )
        except NoResultFound:
            return datetime.date(1900, 1, 1)
        else:
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()


class TimeSeriesBase(Base):
    """"Common time-series capabilities.

    Note
    ----
    This is an abstract base class.

    Note
    ----
    All documented ``Listed`` documented class references may also refer to
    any of ``Listed`` child classes.


    Parameters
    ----------
    asset : .Asset (or polymorph child class)
        The ``Asset`` instance the EOD data belongs to.
    date_stamp : datetime.date
        The end-of-day (EOD) data date stamp.
    """
    # Polymorphism discriminator.
    _discriminator = Column(String(32))

    id = Column(Integer, primary_key=True, autoincrement=True)
    """int: Primary key."""

    _asset_id = Column(Integer, ForeignKey('asset.id'), nullable=False)
    """int: Foreign key giving ``Asset`` a time series capability."""

    date_stamp = Column(Date, nullable=False)
    """datetime: EOD date."""

    __tablename__ = 'time_series_base'
    __mapper_largs__ = {
        'polymorphic_identity': __tablename__,
        'polymorphic_on': _discriminator,
    }
    __table_args__ = (
        UniqueConstraint('_discriminator', '_asset_id', 'date_stamp'),
    )

    date_column_names = ['date_stamp']
    """list: Columns that must be exported externally as pandas.Timestamp."""

    def __init__(self, asset, date_stamp):
        """Instance initialization."""
        self._asset_id = asset.id
        self.date_stamp = date_stamp

    @classmethod
    @property
    def _class_name(cls):
        return cls.__name__

    @classmethod
    def from_data_frame(cls, session, asset_class: Asset, data_frame):
        """Create multiple class instances in the session from a dataframe.

        This method updates all of a specified time series aggregated by the
        ``Listed``  or it's child classes.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        asset_class : .asset.Asset (or child class)
            The ``Asset`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).
        data_frame : pandas.DataFrame
            A ``pandas.DataFrame`` with columns of the same name as this
            class' constructor method arguments, with the exception that instead
            of a column named ``asset``,
            FIXME: instead there must be an ``isin``
            column with the ISIN number of the ``Listed`` instance.

        """

        # Check for zero rows of data
        if data_frame.empty:
            # No data so return
            return

        # The goal is to substitute the `key_code_name` column for the
        # `Asset.id`.
        key_code_name = asset_class.key_code_name
        # Get Asset.key_code to Asset.id translation table
        key_code_id_table = asset_class.key_code_id_table(session)

        data_table = data_frame

        # Guarantee uniqueness in a copy of the data
        data_frame.drop_duplicates(['date_stamp', key_code_name], inplace=True)

        # Guarantee date ranking of the data
        data_table.sort_values(by='date_stamp')

        # Keep only new dated data in the data_table. Due to the behaviour of
        # financial data API services very often return which may include data
        # falling on a date that has already been stored. This may lead to
        # duplicate data which we wish to avoid.
        last_date = TimeSeriesMeta.get_last_date(session, asset_class, cls)
        if last_date is None:
            # No last_date has been set yet as the asset_base is still empty.
            pass
        else:
            last_date = pd.to_datetime(last_date)
            keep_index = pd.to_datetime(last_date) < data_table['date_stamp']
            data_table = data_table.loc[keep_index]

        # Replace pesky pd.NaT with None. Else SqlAlchemy DateTime columns
        # throw (exceptions.TypeError) %d format: a number is required, not
        # float
        data_table.replace({pd.NaT: None}, inplace=True)

        # Join to create a new extended instance_table with the security column.
        # Only for time series instances (left join).
        # FIXME: Warn or raise if left and right are not congruent
        data_table = data_table.merge(
            key_code_id_table, on=key_code_name, how='left')
        data_table.drop(columns=key_code_name, inplace=True)

        instances_list = list()
        data_table.set_index(
            ['id', 'date_stamp'], inplace=True, drop=True)
        # Iterate over all Asset polymorph instances

        # Determine which, if any, security id's are present in the data.
        id_list = data_table.index.to_frame(
            index=False).id.drop_duplicates().to_list()
        # Avoid empty data edge case - with certain date ranges in a data fetch,
        # there may be no new data to be found
        if len(id_list) == 0:
            # Nothing to process so just return
            return
        # Fetch the relevant securities
        security_list = session.query(
            asset_class).filter(asset_class.id.in_(id_list)).all()
        # Add data to each security's time series' asset_class
        for security in security_list:
            # Get the security's time series.
            series = data_table.loc[security.id]
            # Avoid empty series edge case
            if series.empty:
                continue
            # Sort by ascending date_stamp. Use a copy (inplace=False) to avoid
            # a SettingWithCopyWarning
            series.sort_index(inplace=True)
            # Reset date_stamp index making it a column
            series.reset_index(inplace=True)
            # Create the security's series list of class instances and extend
            # onto the instances list
            instances = [
                cls(asset=security, **row) for index, row in series.iterrows()]
            instances_list.extend(instances)

        # The bulk_save_objects does not work with inherited objects. Use
        # add_all instead.
        session.add_all(instances_list)

        # Update the all securities last_date to the latest data_frame
        # date_stamp
        date_stamp = data_frame['date_stamp']
        last_date = date_stamp.sort_values().iloc[-1].to_pydatetime()
        # Make sure the date is a datetime.date instance to avoid a bug due to
        # SQLite allowing the SqlAlchemy `Date` column type to be stored as a
        # `DateTime` column type!!
        TimeSeriesMeta.set_last_date(session, asset_class, cls, last_date)

    @classmethod
    def to_data_frame(cls, session, asset_class):
        """Convert all instances to a single data table.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.

        Returns
        -------
        data_frame : pandas.DataFrame
            A ``pandas.DataFrame`` with columns of the same name as all this
            class' constructor method arguments, with the exception that instead
            of a column named ``listed``, instead there shall be an ``isin``
            column with the ISIN number of the ``Listed`` instance.
        asset_class : .asset.Asset (or child class)
            The ``Asset`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).
        """

        # The goal is to substitute the `key_code_name` column for the
        # `Asset.id`.
        # Get Asset.key_code to Asset.id translation table
        key_code_id_table = asset_class.key_code_id_table(session)

        #  Get a table of time-series instances with attribute columns
        record_list = list()
        for instance in session.query(cls).all():
            # Get instance data dictionary and add the `Listed` ISIN number
            instance_dict = instance.to_dict()
            # Reference to the class primary key attribute Asset.id (or
            # polymorph child class)
            instance_dict['id'] = instance._asset_id
            record_list.append(instance_dict)
        instance_table = pd.DataFrame(record_list)

        # Join in the `id` column. Only for time series instances (left
        # join).
        data_table = instance_table.merge(
            key_code_id_table, on='id', how='left')
        data_table.drop(columns='id', inplace=True)

        # The date_stamp must be pandas.TimeStamp. Note that child classes may
        # redefine the `date_column_names` list.
        for name in cls.date_column_names:
            data_table[name] = pd.to_datetime(data_table[name])

        return data_table

    @classmethod
    def update_all(cls, session, asset_class, get_method, asset_list):
        """ Update/create the eod trade data of all the Listed instances.

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
        asset_class : .asset.Asset (or child class)
            The ``Asset`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).
        asset_list : list of .Asset instances (or child instances)
            The list of assets which must be updated with time series data.

        No object shall be destroyed, only updated, or missing object created.

        """
        # Determine date ranges.
        from_date = TimeSeriesMeta.get_last_date(session, asset_class, cls)
        to_date = datetime.date.today()

        # Get all financial data from the from_date till today.
        data_frame = get_method(asset_list, from_date, to_date)
        # Bulk add/update data.
        cls.from_data_frame(session, asset_class, data_frame)
        # Set Asset class last dates to today
        TimeSeriesMeta.set_last_date(
            session, asset_class, cls, datetime.date.today())

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
        dump_dict[cls._class_name] = cls.to_data_frame(session, asset_class)
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
        class_name = cls._class_name
        data_frame_dict = dumper.read(name_list=[class_name])
        cls.from_data_frame(
            session, asset_class, data_frame_dict[class_name])


class SimpleEOD(TimeSeriesBase):
    """A single listed security's date-stamped EOD trade data.

    Parameters
    ----------
    asset : .Asset or child instance
        The ``.Asset`` instance the EOD data belongs to.
    date_stamp : datetime.date
        The end-of-day (EOD) data date stamp.
    price : float
        Price for the day.

    """

    __tablename__ = 'simple_eod'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    id = Column(Integer, ForeignKey('time_series_base.id'), primary_key=True)
    """ Primary key."""

    price = Column(Float, nullable=False)
    """float: Price for the day."""

    def __init__(
            self, asset, date_stamp, price):
        """Instance initialization."""
        super().__init__(asset, date_stamp)
        self.price = price

    def to_dict(self):
        """Convert all class price attributes to a dictionary."""
        return {
            "date_stamp": self.date_stamp,
            "price": self.open,
        }


class TradeEOD(TimeSeriesBase):
    """A single listed security's date-stamped EOD trade data.

    Parameters
    ----------
    asset : .Asset or child instance
        The ``.Asset`` instance the EOD data belongs to.
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

    __tablename__ = 'trade_eod'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    id = Column(Integer, ForeignKey('time_series_base.id'), primary_key=True)
    """ Primary key."""

    open = Column(Float, nullable=False)
    """float: Open price for the day."""

    close = Column(Float, nullable=False)
    """float: The EOD closing price for the day."""

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

    @property
    def price(self):
        """float: The default price is the close price for EOD prices"""
        return self.close

    def __init__(
            self, asset, date_stamp,
            open, close, high, low, adjusted_close, volume):
        """Instance initialization."""
        super().__init__(asset, date_stamp)
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.adjusted_close = adjusted_close
        self.volume = volume

    def to_dict(self):
        """Convert all class price attributes to a dictionary."""
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
    asset : .Asset or child instance
        The ``.Asset`` instance the EOD data belongs to.
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

    __tablename__ = 'listed_eod'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    id = Column(Integer, ForeignKey('trade_eod.id'), primary_key=True)
    """ Primary key."""

    @classmethod
    def update_all(cls, session, asset_class, get_method):
        """ Update/create the eod trade data of all the Listed instances.

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
        asset_class : .asset.Asset (or child class)
            The ``Asset`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).

        No object shall be destroyed, only updated, or missing object created.

        """
        # TODO: Find a way to get the financial_data module to look for delisted
        # data. Then skipping de-listed securities below can be avoided

        # Skip data fetch and warn for all de-listed securities
        securities_delisted = session.query(asset_class).filter(
            asset_class.status == 'delisted').all()
        for security in securities_delisted:
            logger.warning(
                f'Skipped {cls._class_name} data fetch for '
                f'de-listed security {security.identity_code}.')

        # Get all actively listed Listed instances so we can fetch their
        # EOD trade data
        securities_list = session.query(
            asset_class).filter(asset_class.status == 'listed').all()

        super().update_all(session, asset_class, get_method, securities_list)


class ForexEOD(TradeEOD):
    """A single forex date-stamped EOD trade data.

    Parameters
    ----------
    listed : .Listed
        The ``Listed`` instance the EOD data belongs to.
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

    __tablename__ = 'forex_eod'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    id = Column(Integer, ForeignKey('trade_eod.id'), primary_key=True)
    """ Primary key."""

    def __init__(
            self, asset, date_stamp,
            open, close, high, low, adjusted_close, volume):
        """Instance initialization."""
        super().__init__(
            asset, date_stamp,
            open, close, high, low, adjusted_close, volume)

    @classmethod
    def update_all(cls, session, asset_class, get_method):
        """ Update/create the eod trade data of all the Listed instances.

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
        asset_class : .asset.Asset (or child class)
            The ``Asset`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).

        No object shall be destroyed, only updated, or missing object created.

        """

        # Get all actively listed Listed instances so we can fetch their
        # EOD trade data
        forex_list = session.query(asset_class).all()

        super().update_all(session, asset_class, get_method, forex_list)


class Dividend(TimeSeriesBase):
    """A single listed security's date-stamped EOD trade data.

    Parameters
    ----------
    listed : .Listed
        The ``Listed`` instance the EOD data belongs to.
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

    __tablename__ = 'dividends'
    __mapper_args__ = {'polymorphic_identity': __tablename__, }

    id = Column(Integer, ForeignKey('time_series_base.id'), primary_key=True)
    """ Primary key."""
    # NOTE: Inherited _asset_id backrefs to the polymorphic ListedEquity

    currency = Column(String, nullable=False)
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

    date_column_names = [
        'date_stamp', 'declaration_date', 'payment_date', 'record_date']
    """list: Columns that must be exported externally as pandas.Timestamp."""

    def __init__(
            self, asset, date_stamp,
            currency, declaration_date, payment_date,
            period, record_date, unadjusted_value, adjusted_value, **kwargs):
        """Instance initialization."""
        super().__init__(asset, date_stamp)
        self.currency = currency
        self.declaration_date = declaration_date
        self.payment_date = payment_date
        self.period = period
        self.record_date = record_date
        self.unadjusted_value = unadjusted_value
        self.adjusted_value = adjusted_value

    def to_dict(self):
        """Convert all class dividend attributes to a dictionary."""
        data = {
            "date_stamp": self.date_stamp,
            "currency": self.currency,
            "declaration_date": self.declaration_date,
            "payment_date": self.payment_date,
            "period": self.period,
            "record_date": self.record_date,
            "unadjusted_value": self.unadjusted_value,
            "adjusted_value": self.adjusted_value,
        }

        return data

    @classmethod
    def update_all(cls, session, asset_class, get_method):
        """ Update/create the eod trade data of all the Listed instances.

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
        asset_class : .asset.Asset (or child class)
            The ``Asset`` class which has this time-series data. (Not to be
            confused with the market asset class of security such as cash,
            bonds, equities commodities, etc.).

        No object shall be destroyed, only updated, or missing object created.

        """
        # TODO: Find a way to get the financial_data module to look for delisted
        # data. Then skipping de-listed securities below can be avoided

        # Skip data fetch and warn for all de-listed securities
        securities_delisted = session.query(asset_class).filter(
            asset_class.status == 'delisted').all()
        for security in securities_delisted:
            logger.warning(
                f'Skipped {cls._class_name} data fetch for '
                f'de-listed security {security.identity_code}.')

        # Get all actively listed Listed instances so we can fetch their
        # EOD trade data
        securities_list = session.query(
            asset_class).filter(asset_class.status == 'listed').all()

        super().update_all(session, asset_class, get_method, securities_list)


class LivePrices(Base):
    """Container for live prices
    """
    __tablename__ = 'live_prices'

    date_stamp = Column(Date, nullable=False)
    """datetime: The date-time stamp of the last trade."""

    price = Column(Float, nullable=False)
    """float: The last trade price."""

    open = Column(Float)
    """float: The market opening price."""

    high = Column(Float)
    """float: The current highest price for the day."""

    low = Column(Float)
    """float: The current lowest price for the day."""

    value = Column(Float)
    """float: The value traded for thr last trade."""

    volume = Column(Integer)
    """float: The units traded for the last trade."""

    bid = Column(Float)
    """float: The current highest bid price to buy."""

    offer = Column(Float)
    """float: The current lowest offer price to sell."""

    # There is a one-to-one relationship between listed and LivePrices. Also,
    # the id column needs to be last for the fast insert in the
    # LivePrices.update method to work.
    id = Column(Integer, ForeignKey('asset.id'), primary_key=True)

    def __init__(self, date_stamp, price,
                 open=None, high=None, low=None,
                 value=None, volume=None,
                 bid=None, offer=None,
                 ):
        """Instance initialization."""
        self.date_stamp = date_stamp
        self.price = price
        self.open = open
        self.high = high
        self.low = low
        self.value = value
        self.volume = volume
        self.bid = bid
        self.offer = offer

    def to_dict(self):
        """Convert all class price attributes to a dictionary."""
        return {
            "date_stamp": self.date_stamp,
            "price": self.price,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "value": self.value,
            "volume": self.volume,
            "bid": self.bid,
            "offer": self.offer,
        }

    @classmethod
    def update(cls, session, data_frame):
        """Update multiple class instances price data in the session from a
        data set.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        data_frame : pandas.DataFrame
            A DataFrame object with columns of price data and rows for each
            listed security.
        """
        engine = session.bind

        # From the data_frame create a translation DataFrame of (isin,
        # Asset.id) so we can add and Asset.id column to the data_frame.
        translate_table = list()
        for i, row in data_frame.iterrows():
            # Retrieve the ListedEquity instance by its ISIN and create a
            # (ticker, mic, id) entry. Warn if not found in database.
            try:
                # FIXME: I assume `asset.Listed` means the same as `asset_class`
                entity = asset.Listed.factory(
                    session,
                    ticker=row['ticker'], mic=row['mic'],
                    create=False)
            except FactoryError:
                logger.warning(
                    "LivePrices: Security {mic}.{ticker} "
                    "not found in session.".format(**row.to_dict()))
            else:
                translate_table.append((row['ticker'], row['mic'], entity.id))
        # Create the translation DataFrame
        translate_table = pd.DataFrame(
            translate_table, columns=['ticker', 'mic', 'id'])

        # Join the translate_table to the data_frame so that the data_frame has
        # an id column.
        translate_table.set_index(['mic', 'ticker'], inplace=True)
        data_table = data_frame.join(translate_table, on=['mic', 'ticker'])
        # Drop appropriate columns so that the data_table may be directly
        # updated into LivePrices via the Asset.id column.
        data_table.drop(['mic', 'ticker'], axis=1, inplace=True)

        # Add the data_table to the database as a temporary table. Use the
        # SQLAlchemy CORE operation as we need speed due to the large data sets.
        # Then use the temporary table to update the time-series table.
        data_table.to_sql(con=engine, name='temp_tbl', index=False,
                          if_exists='replace')

        # Insert the data directly using sqlalchemy CORE capability for speed.
        session.execute("INSERT OR REPLACE INTO {0} SELECT * FROM temp_tbl;"
                        .format(LivePrices.__tablename__))
        session.execute("DROP TABLE temp_tbl")
