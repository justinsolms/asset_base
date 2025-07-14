#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Financial data source management.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the asset_base module.
The asset_base module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

This module provides static data for domicile and currency data as well as
securities basic data. The module can use various data financial data feed APIs
but currently only the EOFHistoricalData.com API is implemented. The module
conditions API data with formats and naming conventions appropriate to the
``asset_base`` module requirements. Internally it makes use of renaming
dictionaries for ease of maintenance.


"""
import datetime

import pandas as pd
import os

# Abstract base class.
import abc

from typing import Optional

from .eod_historical_data import Exchanges, MultiHistorical
from .exceptions import _BaseException, TimeSeriesNoData
from .__init__ import get_data_path

# Get module-named logger.
import logging

logger = logging.getLogger(__name__)


class DumpReadError(_BaseException):
    """Dump file not found or could not be read."""


class _Feed(object, metaclass=abc.ABCMeta):
    """Financial data feed abstract base class.

    Parameters
    ----------
    testing : bool, optional
        Set to `True` for testing. This controls specifics in a way that
        avoids testing data clashes with, and possible overwriting of, the
        operational data.

    """

    _CLASS_TEST_DATA_PATH = "test_data_path"

    @abc.abstractmethod
    def get_class_data_path(self) -> Optional[str]:
        """Abstract method to get the class data path."""
        pass

    def __init__(self, testing=False):
        """Instance initialization."""
        # Avoid conflict and overwriting with operational and testing data
        if testing:
            self._class_data_path = self._CLASS_TEST_DATA_PATH
        else:
            self._class_data_path = self.get_class_data_path()

        # Do not call superclass __init__ as its messes with the paths. Here we
        # use the `var` path instead of the `data` path.

        # Make the absolute var path for writing. Overwrites the
        # `_abs_data_path` of the parent class.
        if self._class_data_path is not None:
            self._abs_data_path = get_data_path(self._class_data_path)
            self.makedir()

    def _path(self, file_name: str = "") -> str:
        """Absolute data path schema with optional file name.

        Parameters
        ----------
        file_name : str, optional
            Return the name of the file to be found at the full path. If none
            is provided then only the folder path is returned.
        """
        if not hasattr(self, "_abs_data_path"):
            raise RuntimeError(
                "The _abs_data_path attribute is not set. "
                "Call the makedir() method to set it."
            )
        if file_name:
            path = os.path.join(self._abs_data_path, file_name)
        else:
            path = self._abs_data_path

        return path

    def makedir(self):
        """Make path if not exist."""
        # Note that not all subclasses have class data path set
        class_data_path = self.get_class_data_path()
        abs_data_path = get_data_path(class_data_path)
        # Make directory if not existing
        if not os.path.isdir(abs_data_path):
            logger.debug("Created folder %s", abs_data_path)
            os.makedirs(abs_data_path)
        # The path will now exist
        self._abs_data_path = abs_data_path


class Dump(_Feed):
    """Dump and read re-usable content to and from disk files.

    The destination folder is `data/dump`.

    The dumper uses a dump_dict : dict of pandas.DataFrame, with the key the
    CSV dump file prefix when dumping (writing), for the same reason it uses
    a list of file name prefixes for reading. The reason is that this class
    was originally designed to work with multiple class dumps at once.

    """
    _CLASS_TEST_DATA_PATH = "testing_dumps"

    def get_class_data_path(self) -> Optional[str]:
        """Return the class data path."""
        return "dumps"

    def __init__(self, testing=False):
        """Instance initialization.

        Parameters
        ----------
        testing : bool, optional
            Set to `True` for testing. This controls specifics in a way that
            avoids testing data clashes with, and possible overwriting of, the
            operational data.

        """
        super().__init__(testing=testing)

    def write(self, dump_dict):
        """Write a dict of ``pandas.DataFrame`` to CSV files.

        The key is the file prefix.

        Parameters
        ----------
        dump_dict : dict of pandas.DataFrame
            The key shall be the CSV file prefix
        """
        for key, item in list(dump_dict.items()):
            file_name = f"{key}.pandas.dataframe.pkl"
            path = self._path(file_name)
            item.to_pickle(path)
            logger.info(f"Dumped class {key} to {path}")

    def read(self, name_list):
        """Read a dict of ``pandas.DataFrame`` from CSV files.

        The file prefix is the key.

        Parameters
        ----------
        name_list : list
            Names of CSV file name prefixes.

        Returns
        -------
        dict of pandas.DataFrame
            The key shall be the CSV file name prefix
        """
        dump_dict = dict()
        for name in name_list:
            file_name = f"{name}.pandas.dataframe.pkl"
            path = self._path(file_name)
            dump_dict[name] = pd.read_pickle(path)
            logger.debug("Read dump file %s", path)

        return dump_dict

    def delete(self):
        """Delete the dump folder contents.

        Note
        ----
        The dump folder is NOT deleted as this is too destructive and could
        cause issues with methods that assume a valid folder. The folder is kept
        but its contents are deleted.

        """
        path = self._path()
        if not os.path.exists(path):
            # Check that the dump folder exists as it should have been created
            # at class initialization
            raise RuntimeError(
                "The dump folder does not exist. "
                "The makedir() call from the  __init__ method should have created it."
            )

        # First delete all the folder content files.
        file_name_list = os.listdir(path)
        for file_name in file_name_list:
            path = self._path(file_name)
            os.remove(path)
            logger.debug("Deleted dump file %s", path)

    def exists(self, dump_class):
        """Verify that a dump file exits for a dumped class.

        Warning
        -------
        An important use is to verify that dump files were created before
        tearing down a database.

        Parameters
        ----------
        dump_class : .asset.Asset or child class
            The class for which the dump files existence must be verified. The
            class must have a valid ``dump`` method.
        """
        file_name = f"{dump_class._class_name}.pandas.dataframe.pkl"
        path = self._path(file_name)

        return os.path.exists(path)


class Static(_Feed):
    """Static data feed class.

    This class provides static data needed for the ``asset`` module such as:

        * Currency information.
        * Domicile information.
        * World exchange information.

    """

    def get_class_data_path(self) -> Optional[str]:
        """Return the class data path."""
        return "static"

    def __init__(self):
        """Instance initialization."""
        super().__init__()

    def get_currency(self):
        """Fetch currencies from the local file."""
        file_name = "CurrencyCountry.csv"
        path = self._path(file_name)

        column_dict = {
            "CurrencyCode": "ticker",
            "CurrencyName": "name",
            "CountryCode": "country_code",
        }

        # Read the data. # Gotcha: CountryCode "NA" for Namibia in csv becomes
        # NaN.
        logger.debug("Fetching currency data from {}.".format(path))
        data = pd.read_csv(path, na_filter=False)

        # Extract by columns name and rename to a standard. This is also then a
        # check for expected columns.
        data = data[list(column_dict.keys())]
        data.rename(columns=column_dict, inplace=True)

        # Convert multiple country codes to a list.
        country_codes_list = list()
        data_thinned = data.drop_duplicates(subset="ticker").copy()
        for ticker in data_thinned["ticker"]:
            mask = data["ticker"] == ticker
            country_codes = data[mask]["country_code"].tolist()
            country_codes = ",".join(country_codes)
            country_codes_list.append(country_codes)
        data_thinned["country_code_list"] = country_codes_list
        data = data_thinned.drop(columns="country_code")

        return data

    def get_domicile(self):
        """Fetch domicile from the local file."""
        file_name = "CurrencyCountry.csv"
        path = self._path(file_name)

        column_dict = {
            "CountryCode": "country_code",
            "CountryName": "country_name",
            "CurrencyCode": "currency_ticker",
            # Currency name not important here as all currency instances exit
        }

        # Read the data. # Gotcha: CountryCode "NA" for Namibia in csv becomes
        # NaN.
        logger.debug("Fetching domicile data from {}.".format(path))
        data = pd.read_csv(path, na_filter=False)

        # Extract by columns name and rename to a standard. This is also then a
        # check for expected columns.
        data = data[list(column_dict.keys())]
        data.rename(columns=column_dict, inplace=True)

        return data

    def get_exchange(self):
        """Fetch exchanges from the local file."""
        file_name = "EODExchanges.csv"
        path = self._path(file_name)

        column_dict = {
            "MIC": "mic",
            "CountryCode": "country_code",
            "ExchangeName": "exchange_name",
            "EODCode": "eod_code",
        }

        # Read the data. # Gotcha: CountryCode "NA" for Namibia in csv becomes
        # NaN.
        logger.debug("Fetching exchange data from {}.".format(path))
        data = pd.read_csv(path, na_filter=False)

        # Extract by columns name and rename to a standard. This is also then a
        # check for expected columns.
        data = data[list(column_dict.keys())]
        data.rename(columns=column_dict, inplace=True)

        return data


class StaticIndices(_Feed):
    """Static index data reading class for reading indices from data files.

    This class provides static data needed for the ``asset`` module such as:

        * Index mete-data.
        * Index time series.

    """

    def get_class_data_path(self) -> Optional[str]:
        """Return the class data path."""
        return "static_time_series"

    def __init__(self):
        """Instance initialization."""
        super().__init__()

    def get_indices_meta(self, **kwargs):
        """Fetch indices mete data from the feeds."""
        file_name = "IndicesMeta.csv"
        path = self._path(file_name)

        column_dict = {
            "Name": "index_name",
            "Code": "ticker",
            "Currency": "currency_code",
            "TRI": "total_return",
        }

        with open(path) as stream:
            data = pd.read_csv(stream)

        # If no data then just return a simple empty pandas DataFrame.
        if data.empty:
            raise Exception(f"Expected index data but found file {path} empty.")
        #  Reset the index as we need to form here on treat the index as
        # column data. The security and date info is in the index
        data.reset_index(inplace=True)
        # Extract by columns name and rename to a standard. This is also
        # then a check for expected columns.
        data = data[list(column_dict.keys())]
        data.rename(columns=column_dict, inplace=True)

        # Mark the data as static (non feed API).
        data["static"] = True

        # For testing purposes only!!!
        if "_test_ticker_list" in kwargs and kwargs["_test_ticker_list"] is not None:
            _test_ticker_list = kwargs.pop("_test_ticker_list")
            # Don't confuse the ISIN column with pandas DataFrame.isin (is-in)!
            data = data[data["ticker"].isin(_test_ticker_list)]

        return data

    def get_indices(self, index_list, **kwargs):
        """Get historical EOD for a specified list of indices.

        This method fetches the data from the specified feed.

        index_list : list of .asset_base.Index instances
            A list of forex for which data are required.

        """
        # Column renaming dict
        column_dict = {
            "Name": "index_name",
            "Code": "ticker",
            "Currency": "currency_code",
            "TRI": "total_return",
        }

        # Look for files with INDX.<index ticker>.csv
        data_list = list()
        tickers = [index.ticker for index in index_list]
        for ticker in tickers:
            file_name = f"INDX.{ticker}.csv"
            path = self._path(file_name)
            # Read CSV history file
            with open(path) as file:
                data = pd.read_csv(file)
            # Try extract by columns names and rename to a standard. This is
            # also then a check for expected columns.
            try:
                data = data[list(column_dict.keys())]
            except KeyError:
                raise KeyError(f"Error in column headers for file {path}")
            else:
                data.rename(columns=column_dict, inplace=True)
            # Append to list for concatenation into one pandas.DataFrame
            data_list.append(data)
        data = pd.concat(data_list, axis="index")
        # Condition date
        data["date_stamp"] = pd.to_datetime(data["date_stamp"])

        return data


class MetaData(_Feed):
    """Provide fundamental and meta-data of the working universe securities."""

    def get_class_data_path(self) -> Optional[str]:
        """Return the class data path."""
        return "static"

    def __init__(self):
        """Instance initialization."""
        super().__init__()

    def get_etfs(self, **kwargs):
        """Fetch JSE securities mata-data from a local file."""
        universe_file_name = "ETFMeta.csv"
        path = self._path(universe_file_name)

        column_dict = {
            "mic": "mic",
            "ticker": "ticker",
            "name": "listed_name",
            "status": "status",
            "distributions": "distributions",
            "asset_class": "asset_class",
            "locality": "locality",
            "domicile": "domicile_code",
            "quote_units": "quote_units",
            "industry_class": "industry_class",
            "industry_code": "industry_code",
            "industry_name": "industry_name",
            "isin": "isin",
            "issuer_domicile": "issuer_domicile_code",
            "issuer_name": "issuer_name",
            "sector_code": "sector_code",
            "sector_name": "sector_name",
            "sub_sector_code": "sub_sector_code",
            "sub_sector_name": "sub_sector_name",
            "super_sector_code": "super_sector_code",
            "super_sector_name": "super_sector_name",
            "ter": "ter",
        }

        # Read the data. # Gotcha: CountryCode "NA" for Namibia in csv becomes
        # NaN.
        logger.debug("Fetching JSE ETFs meta-data from {}.".format(path))

        # Read data with proper dat typing
        data = pd.read_csv(
            path,
            na_filter=False,
        )

        # Extract by columns name and rename to a standard. This is also then a
        # check for expected columns.
        data = data[list(column_dict.keys())]
        data.rename(columns=column_dict, inplace=True)

        # For testing purposes only!!!
        if "_test_isin_list" in kwargs and kwargs["_test_isin_list"] is not None:
            _test_isin_list = kwargs.pop("_test_isin_list")
            # Don't confuse the ISIN column with pandas DataFrame.isin (is-in)!
            data = data[data["isin"].isin(_test_isin_list)]

        return data

    def get_indices(self, feed="EOD", **kwargs):
        """Fetch indices mete data from the feeds."""

        if feed == "EOD":
            feed = Exchanges()
            column_dict = {
                "Name": "index_name",
                "Code": "ticker",
                "Currency": "currency_code",
            }
            # Try fetch the data form the feed
            data = feed.get_indices()
            logger.debug("Got Indices meta data.")
            # If no data then just return a simple empty pandas DataFrame.
            if data.empty:
                return pd.DataFrame
            #  Reset the index as we need to form here on treat the index as
            # column data. The security and date info is in the index
            data.reset_index(inplace=True)
            # Extract by columns name and rename to a standard. This is also
            # then a check for expected columns.
            data = data[list(column_dict.keys())]
            data.rename(columns=column_dict, inplace=True)
        else:
            raise Exception("Feed {} not implemented.".format(feed))

        # For testing purposes only!!!
        if "_test_ticker_list" in kwargs and kwargs["_test_ticker_list"] is not None:
            _test_ticker_list = kwargs.pop("_test_ticker_list")
            # Don't confuse the ISIN column with pandas DataFrame.isin (is-in)!
            data = data[data["ticker"].isin(_test_ticker_list)]

        return data

    # TODO: Add get_exchanges method
    # TODO: Add get_exchange_symbols method


class History(_Feed):
    """Provide securities historical data from data feeds.

    This class manages
    """

    def get_class_data_path(self) -> str:
        """Return the class data path."""
        return ""

    def __init__(self):
        """Instance initialization."""
        super().__init__()

    @staticmethod
    def date_preprocessor(obj_list, from_date, to_date, series):
        """Get date ranges based on arguments and last available data series.

        Parameters
        ----------
        obj_list : .asset.Base (or polymorph child class)
            The list of asset instances for which the last data series date
            should be queried.
        from_date : datetime.date
            If provided then a list of `len(obj_list) * [from_date]` is
            returned. If a ``from_date`` argument is not provided then the time
            series of each ``Asset`` in the ``obj_list`` is inspected and the
            ``from_date`` generated according to the latest available data date
            of the ``Asset``'s time series.
        from_date : datetime.date
            If provided then a list of `len(obj_list) * [to_date]` is returned.
            If not provided then the date returned shall be that of today.
        series : str
            A string indicator of the type of series required ('eod',
            'dividend', 'forex', 'index').

        Returns
        -------
        from_date_list
            The list of `from_dates` the length of ``obj_list``.
        to_date_list
            The list of `to_dates` the length of ``obj_list``.

        """
        # From date list, one per Asset instance
        if from_date is None:
            # for each `Asset` object in the list default to the last data date
            # for the Asset object
            from_date_list = list()
            for asset in obj_list:
                try:
                    if series in ["eod", "forex", "index"]:
                        from_date = asset.get_last_eod_date()
                    elif series in ["dividend"]:
                        from_date = asset.get_last_dividend_date()
                    else:
                        raise ValueError(
                            f"Unexpected value {series} for `series` argument."
                        )
                except TimeSeriesNoData:
                    from_date = None
                from_date_list.append(from_date)
        else:
            from_date_list = [from_date] * len(obj_list)

        if to_date is None:
            # Default to today
            to_date_list = [datetime.date.today()] * len(obj_list)
        else:
            to_date_list = [to_date] * len(obj_list)

        return from_date_list, to_date_list

    def get_eod(self, asset_list, from_date=None, to_date=None, feed="EOD"):
        """Get historical EOD for a specified list of securities.

        This method fetches the data from the specified feed.

        asset_list : list of .asset_base.Listed or child classes
            A list of securities that are listed and traded.
        from_date : datetime.date, optional
            Inclusive start date of historical data. If not provided then the
            date is set to the ``asset.Asset.get_last_eod_date()`` date for each
            asset in the ``asset_list`` argument.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.
        feed : str
            The data feed module to use:
                'EOD' - eod_historical_data

        """
        # Generate (or default) date list with date ranges for each asset.
        from_date_list, to_date_list = self.date_preprocessor(
            asset_list, from_date, to_date, series="eod"
        )

        # Assemble symbol list
        symbol_list = list()
        for sec, from_date, to_date in zip(asset_list, from_date_list, to_date_list):
            symbol_list.append((sec.ticker, sec.exchange.eod_code, from_date, to_date))

        # Pick feed
        if feed == "EOD":
            feed = MultiHistorical()
            column_dict = {
                "date": "date_stamp",
                "ticker": "ticker",
                "exchange": "mic",
                "adjusted_close": "adjusted_close",
                "close": "close",
                "high": "high",
                "low": "low",
                "open": "open",
                "volume": "volume",
            }
            data = feed.get_eod(symbol_list)
            logger.debug("Got EOD data.")
            # If no data then just return a simple empty pandas DataFrame.
            if data.empty:
                return pd.DataFrame
            #  Reset the index as we need to form here on treat the index as
            # column data. The security and date info is in the index
            data.reset_index(inplace=True)
            # Extract by columns name and rename to a standard. This is also
            # then a check for expected columns.
            data = data[list(column_dict.keys())]
            data.rename(columns=column_dict, inplace=True)
            # Replace EODHistoricalData.com's exchange codes (the mic column)
            # with exchange MICs
            eod_to_mic_dict = dict(
                [(s.exchange.eod_code, s.exchange.mic) for s in asset_list]
            )
            data.replace({"mic": eod_to_mic_dict}, inplace=True)
            # Augment the ticker-mic with the matching ISIN code
            mic_ticker_to_isin_dict = dict(
                [((s.exchange.mic, s.ticker), s.isin) for s in asset_list]
            )
            data["_key"] = data[["mic", "ticker"]].to_records(index=False).tolist()
            data["isin"] = data["_key"].map(mic_ticker_to_isin_dict)
            # These columns are expected by asset_base
            data.drop(columns=["_key", "mic", "ticker"], inplace=True)
            # Condition date
            data["date_stamp"] = pd.to_datetime(data["date_stamp"])
        else:
            raise Exception("Feed {} not implemented.".format(feed))

        return data

    def get_dividends(self, asset_list, from_date=None, to_date=None, feed="EOD"):
        """Get historical dividends for a list of securities.

        This method fetches the data from the specified feed.

        asset_list : list of .asset_base.Listed or child classes A list of
            securities that are listed and traded.
        from_date : datetime.date, optional
            Inclusive start date of historical data. If not provided then the
            date is set to the ``asset.ListedEquity.get_last_dividend_date()``
            date for each asset in the ``asset_list`` argument.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.
        feed : str
            The data feed module to use:
                'EOD' - eod_historical_data

        """
        # Generate date list with date ranges for each asset.
        from_date_list, to_date_list = self.date_preprocessor(
            asset_list, from_date, to_date, series="dividend"
        )

        # Assemble symbol list
        symbol_list = list()
        for sec, from_date, to_date in zip(asset_list, from_date_list, to_date_list):
            symbol_list.append((sec.ticker, sec.exchange.eod_code, from_date, to_date))

        # Pick feed
        if feed == "EOD":
            feed = MultiHistorical()
            column_dict = {
                "date": "date_stamp",
                "ticker": "ticker",
                "exchange": "mic",
                "currency": "currency",
                "declarationDate": "declaration_date",
                "paymentDate": "payment_date",
                "period": "period",
                "recordDate": "record_date",
                "unadjustedValue": "unadjusted_value",
                "value": "adjusted_value",
            }
            date_columns_list = [
                "date_stamp",
                "declaration_date",
                "payment_date",
                "record_date",
            ]
            data = feed.get_dividends(symbol_list)
            logger.debug("Got dividend data.")
            # If no data then just return a simple empty pandas DataFrame.
            if data.empty:
                return pd.DataFrame
            #  Reset the index as we need to from here on treat the index as
            # column data. The security and date info is in the index
            data.reset_index(inplace=True)
            # Extract by columns name and rename to a standard. This is also
            # then a check for expected columns.
            data = data[list(column_dict.keys())]
            data.rename(columns=column_dict, inplace=True)
            # Replace EODHistoricalData.com's exchange codes (the mic column)
            # with exchange MICs
            eod_to_mic_dict = dict(
                [(s.exchange.eod_code, s.exchange.mic) for s in asset_list]
            )
            data.replace({"mic": eod_to_mic_dict}, inplace=True)
            # Augment the ticker-mic with the matching ISIN code using a mapping
            # dictionary
            mic_ticker_to_isin_dict = dict(
                [((s.exchange.mic, s.ticker), s.isin) for s in asset_list]
            )
            data["_key"] = data[["mic", "ticker"]].to_records(index=False).tolist()
            data["isin"] = data["_key"].map(mic_ticker_to_isin_dict)
            # These columns are expected by asset_base
            data.drop(columns=["_key", "mic", "ticker"], inplace=True)
            # Condition date
            for column in date_columns_list:
                data[column] = pd.to_datetime(data[column])
        else:
            raise Exception("Feed {} not implemented.".format(feed))

        return data

    def get_forex(self, forex_list, from_date=None, to_date=None, feed="EOD"):
        """Get historical EOD for a specified list of securities.

        This method fetches the data from the specified feed.

        forex_list : list of .asset_base.Forex instances
            A list of forex for which data are required.
        from_date : datetime.date, optional
            Inclusive start date of historical data. If not provided then the
            date is set to the ``asset.Forex.get_last_eod_date()`` date for each
            asset in the ``asset_list`` argument.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.
        feed : str
            The data feed module to use:
                'EOD' - eod_historical_data

        """
        # Generate date list with date ranges for each asset.
        from_date_list, to_date_list = self.date_preprocessor(
            forex_list, from_date, to_date, series="forex"
        )

        # Assemble symbol list
        symbol_list = list()
        for sec, from_date, to_date in zip(forex_list, from_date_list, to_date_list):
            symbol_list.append((sec.ticker, from_date, to_date))

        # Pick feed
        if feed == "EOD":
            feed = MultiHistorical()
            column_dict = {
                "date": "date_stamp",
                "ticker": "ticker",
                "adjusted_close": "adjusted_close",
                "close": "close",
                "high": "high",
                "low": "low",
                "open": "open",
                "volume": "volume",
            }
            data = feed.get_forex(symbol_list)
            logger.debug("Got Forex data.")
            # If no data then just return a simple empty pandas DataFrame.
            if data.empty:
                return pd.DataFrame
            #  Reset the index as we need to from here on treat the index as
            # column data. The security and date info is in the index
            data.reset_index(inplace=True)
            # Extract by columns name and rename to a standard. This is also
            # then a check for expected columns.
            data = data[list(column_dict.keys())]
            data.rename(columns=column_dict, inplace=True)
            # Condition date
            data["date_stamp"] = pd.to_datetime(data["date_stamp"])
        else:
            raise Exception("Feed {} not implemented.".format(feed))

        return data

    def get_indices(self, index_list, from_date=None, to_date=None, feed="EOD"):
        """Get historical EOD for a specified list of indices.

        This method fetches the data from the specified feed.

        index_list : list of .asset_base.Index instances
            A list of forex for which data are required.
        from_date : datetime.date, optional
            Inclusive start date of historical data. If not provided then the
            date is set to the ``asset.Forex.get_last_eod_date()`` date for each
            asset in the ``asset_list`` argument.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.
        feed : str
            The data feed module to use:
                'EOD' - eod_historical_data

        """
        # Generate date list with date ranges for each asset.
        from_date_list, to_date_list = self.date_preprocessor(
            index_list, from_date, to_date, series="index"
        )

        # Assemble symbol list
        symbol_list = list()
        for sec, from_date, to_date in zip(index_list, from_date_list, to_date_list):
            symbol_list.append((sec.ticker, from_date, to_date))

        # Pick feed
        if feed == "EOD":
            feed = MultiHistorical()
            column_dict = {
                "date": "date_stamp",
                "ticker": "ticker",
                "adjusted_close": "adjusted_close",
                "close": "close",
                "high": "high",
                "low": "low",
                "open": "open",
                "volume": "volume",
            }
            data = feed.get_index(symbol_list)
            logger.debug("Got Forex data.")
            # If no data then just return a simple empty pandas DataFrame.
            if data.empty:
                return pd.DataFrame
            #  Reset the index as we need to from here on treat the index as
            # column data. The security and date info is in the index
            data.reset_index(inplace=True)
            # Extract by columns name and rename to a standard. This is also
            # then a check for expected columns.
            data = data[list(column_dict.keys())]
            data.rename(columns=column_dict, inplace=True)
            # Condition date
            data["date_stamp"] = pd.to_datetime(data["date_stamp"])
        else:
            raise Exception("Feed {} not implemented.".format(feed))

        return data
