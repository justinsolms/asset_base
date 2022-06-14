#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Financial data source management.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the fundmanage module.
The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

This module provides static data for domicile and currency data as well as
securities basic data. The module can use various data financial data feed APIs
but currently only the EOFHistoricalData.com API is implemented. The module
conditions API data with formats and naming conventions appropriate to the
``asset_base`` module requirements. Internally it makes use of renaming
dictionaries for ease of maintenance.


"""
import asset_base.eod_historical_data as eod

import pandas as pd
import os

# Abstract base class.
import abc

# Get module-named logger.
import logging
logger = logging.getLogger(__name__)


class _Feed(object, metaclass=abc.ABCMeta):
    """Generic financial data feed class.

    Parameters
    ----------
    asset_base : fundmanage.asset_base.EntityBase
        An ``asset_base`` database manager with a session to the database.
    """

    def _path(self, file_name=None):
        """Mandatory data path schema.

        Parameters
        ----------
        file_name : str, optional
            Return the name of the file to be found at the full path. If none
            is provided then only the folder path is returned.
        """
        if file_name is None:
            path = os.path.join(
                os.getcwd(),
                self._data_path,
                self._sub_path,
                )
        else:
            path = os.path.join(
                os.getcwd(),
                self._data_path,
                self._sub_path,
                file_name)

        return path

    def makedir(self):
        """Make path if not exist."""
        path = self._path()
        if not os.path.isdir(path):
            logger.info('Created folder %s', path)
            os.makedirs(path)


class Dump(_Feed):
    """Dump and read re-usable content to and from disk files.

        The destination folder is `data/dump`.

        The dumper uses a dump_dict : dict of pandas.DataFrame, with the key the
        CSV dump file prefix when dumping (writing), for the same reason it uses
        a list of file name prefixes for reading. The reason is that this class
        was originally designed to work with multiple class dumps at once.

    """
    def __init__(self, testing=False):
        """Instance initialization.

        Parameters
        ----------
        testing : bool, optional
            Set to `True` for testing. This controls specifics in a way that
            avoids testing dump data clashes with, and possible overwriting of,
            the operational dump data.

        """
        super().__init__()

        # Avoid conflict and overwriting with operational and testing data
        if not testing:
            self._data_path = "data"
            self._sub_path = "dumps"
        else:
            self._data_path = "data"
            self._sub_path = "test_dumps"

        # Make the data path for writing
        self.makedir()

    def write(self, dump_dict):
        """Write a dict of ``pandas.DataFrame`` to CSV files.

        The key is the file prefix.

        Parameters
        ----------
        dump_dict : dict of pandas.DataFrame
            The key shall be the CSV file prefix
        """
        for key, item in list(dump_dict.items()):
            file_name = f'{key}.pandas.dataframe.pkl'
            path = self._path(file_name)
            try:
                item.to_pickle(path)
            except Exception:
                logger.error('Could not write dump file %s', path)
                raise
            else:
                logger.info('Wrote dump file %s', path)

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
            file_name = f'{name}.pandas.dataframe.pkl'
            path = self._path(file_name)
            try:
                dump_dict[name] = pd.read_pickle(path)
            except Exception:
                logger.warning('Could not read dump file %s', path)
            else:
                logger.info('Read dump file %s', path)

        return dump_dict

    def delete(self, delete_folder=True):
        """Delete the dump folder and its contents.

        Parameters
        ----------
        delete_folder : bool, optional
            When set to `True` (default) then the dump folder and its contents
            are  deleted too.  If set `False` then the folder is kept but its
            content are deleted.

        """
        # List all files in folder.
        path = self._path()
        files = os.listdir(path)

        # First delete all the folder content files.
        for file_name in files:
            path = self._path(file_name)
            try:
                os.remove(path)
            except Exception:
                logger.warning('Could not delete dump file %s', path)
            else:
                logger.info('Deleted dump file %s', path)

        # If required then delete the now empty folder too.
        if delete_folder:
            # Delete containing folder
            path = self._path()
            try:
                os.rmdir(path)
            except Exception:
                logger.warning('Could not delete dump folder %s', path)
            else:
                logger.info('Deleted dump folder %s', path)

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
        file_name = f'{dump_class._class_name}.pandas.dataframe.pkl'
        path = self._path(file_name)

        return os.path.exists(path)


class Static(_Feed):
    """Static data feed class.

    This class provides static data needed for the `asset_base` module such as:

        * Currency information.
        * Domicile information.
        * World exchange information.

    """

    def __init__(self):
        """Instance initialization."""
        super().__init__()
        self._data_path = "data"
        self._sub_path = "static"

    def get_currency(self):
        """Fetch currencies from the local file."""
        file_name = 'Currency.csv'
        path = self._path(file_name)

        column_dict = {
            'CurrencyCode': 'ticker',
            'CurrencyName': 'name',
        }

        # Read the data. # Gotcha: CountryCode "NA" for Namibia in csv becomes
        # NaN.
        logger.info('Fetching currency data from {}.'.format(path))
        data = pd.read_csv(path, na_filter=False)

        # Extract by columns name and rename to a standard. This is also then a
        # check for expected columns.
        data = data[list(column_dict.keys())]
        data.rename(columns=column_dict, inplace=True)
        # Multiple countries often use a common currency
        data.drop_duplicates(subset='ticker', inplace=True)

        return data

    def get_domicile(self):
        """Fetch domicile from the local file."""
        file_name = 'CurrencyCountry.csv'
        path = self._path(file_name)

        column_dict = {
            'CountryCode': 'country_code',
            'CountryName': 'country_name',
            'CurrencyCode': 'currency_ticker',
            # Currency name not important here as all currency instances exit
        }

        # Read the data. # Gotcha: CountryCode "NA" for Namibia in csv becomes
        # NaN.
        logger.info('Fetching domicile data from {}.'.format(path))
        data = pd.read_csv(path, na_filter=False)

        # Extract by columns name and rename to a standard. This is also then a
        # check for expected columns.
        data = data[list(column_dict.keys())]
        data.rename(columns=column_dict, inplace=True)

        return data

    def get_exchange(self):
        """Fetch exchanges from the local file."""
        file_name = 'EODExchanges.csv'
        path = self._path(file_name)

        column_dict = {
            'MIC': 'mic',
            'CountryCode': 'country_code',
            'ExchangeName': 'exchange_name',
            'EODCode': 'eod_code',
        }

        # Read the data. # Gotcha: CountryCode "NA" for Namibia in csv becomes
        # NaN.
        logger.info('Fetching exchange data from {}.'.format(path))
        data = pd.read_csv(path, na_filter=False)

        # Extract by columns name and rename to a standard. This is also then a
        # check for expected columns.
        data = data[list(column_dict.keys())]
        data.rename(columns=column_dict, inplace=True)

        return data


class SecuritiesFundamentals(_Feed):
    """Provide fundamental and meta-data of the working universe securities."""

    def __init__(self):
        """Instance initialization."""
        super().__init__()
        self._sub_path = "static"
        self._data_path = "data"

    def get_securities(self, **kwargs):
        """Fetch JSE securities mata-data from a local file.

        Note
        ----
        The _test_isin_list argument is for testing only. Please do not use it.

        """
        universe_file_name = 'ETFs.JSE.Meta.csv'
        path = self._path(universe_file_name)

        column_dict = {
            'mic': 'mic',
            'ticker': 'ticker',
            'name': 'listed_name',
            'status': 'status',
            'asset_class': 'asset_class',
            'locality': 'locality',
            'domicile': 'domicile_code',
            'quote_units': 'quote_units',
            'exchange_board': 'exchange_board',
            'industry_class': 'industry_class',
            'industry_code': 'industry_code',
            'industry_name': 'industry_name',
            'isin': 'isin',
            'issuer_domicile': 'issuer_domicile_code',
            'issuer_name': 'issuer_name',
            'alt_name': 'alt_name',
            'roll_up': 'roll_up',
            'sector_code': 'sector_code',
            'sector_name': 'sector_name',
            'sub_sector_code': 'sub_sector_code',
            'sub_sector_name': 'sub_sector_name',
            'super_sector_code': 'super_sector_code',
            'super_sector_name': 'super_sector_name',
            'ter': 'ter',
        }

        # Read the data. # Gotcha: CountryCode "NA" for Namibia in csv becomes
        # NaN.
        logger.info('Fetching JSE ETFs meta-data from {}.'.format(path))

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
        if '_test_isin_list' in kwargs and \
                kwargs['_test_isin_list'] is not None:
            _test_isin_list = kwargs['_test_isin_list']
            # Don't confuse the ISIN column with pandas DataFrame.isin (is-in)!
            data = data[data['isin'].isin(_test_isin_list)]

        return data


class SecuritiesHistory(_Feed):
    """Provide securities historical data from data feeds.

    """
    def __init__(self):
        """Instance initialization."""
        super().__init__()
        self._data_path = None
        self._sub_path = None

    def get_eod(
            self, securities_list, from_date=None, to_date=None, feed='EOD'):
        """ Get historical EOD for a specified list of securities.

        This method fetches the data from the specified feed.

        securities_list : list of .asset_base.Listed or child classes
            A list of securities that are listed and traded.
        from_date : datetime.date
            Inclusive start date of historical data. If not provided then the
            date is set to 1900-01-01.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.
        feed : str
            The data feed module to use:
                'EOD' - eod_historical_data

        """
        if feed == 'EOD':
            column_dict = {
                'date': 'date_stamp',
                'ticker': 'ticker',
                'exchange': 'mic',
                'adjusted_close': 'adjusted_close',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'open': 'open',
                'volume': 'volume',
            }
            symbol_list = [
                (s.ticker, s.exchange.eod_code) for s in securities_list]
            feed = eod.BulkHistorical()
            try:
                data = feed.get_eod(symbol_list, from_date, to_date)
            except Exception as ex:
                logger.error('Failed to get EOD data.')
                raise ex
            # If no data then just return a simple empty pandas DataFrame.
            if data.empty:
                return pd.DataFrame
            #  Reset the index as we need to form here on treat the index as
            # column data.
            data.reset_index(inplace=True)
            # Extract by columns name and rename to a standard. This is also
            # then a check for expected columns.
            data = data[list(column_dict.keys())]
            data.rename(columns=column_dict, inplace=True)
            # Replace EODHistoricalData.com's exchange codes (the mic column)
            # with exchange MICs
            eod_to_mic_dict = dict([
                (s.exchange.eod_code, s.exchange.mic) for s in securities_list])
            data.replace({'mic': eod_to_mic_dict}, inplace=True)
            # Augment the ticker-mic with the matching ISIN code
            mic_ticker_to_isin_dict = dict([
                ((s.exchange.mic, s.ticker), s.isin) for s in securities_list])
            data['_key'] = data[['mic', 'ticker']
                                ].to_records(index=False).tolist()
            data['isin'] = data['_key'].map(mic_ticker_to_isin_dict)
            # These columns are expected by asset_base
            data.drop(columns=['_key', 'mic', 'ticker'], inplace=True)
            # Condition date
            data['date_stamp'] = pd.to_datetime(data['date_stamp'])
        else:
            raise Exception('Feed {} not implemented.'.format(feed))

        return data

    def get_dividends(
            self, securities_list, from_date=None, to_date=None, feed='EOD'):
        """ Get historical dividends for a list of securities.

        This method fetches the data from the specified feed.

        securities_list : list of .asset_base.Listed or child classes A list of
            securities that are listed and traded. from_date : datetime.date
            Inclusive start date of historical data. If not provided then the
            date is set to 1900-01-01. to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today. feed : str The data feed module to use: 'EOD' -
            eod_historical_data

        """
        if feed == 'EOD':
            column_dict = {
                'date': 'date_stamp',
                'ticker': 'ticker',
                'exchange': 'mic',
                'currency': 'currency',
                'declarationDate': 'declaration_date',
                'paymentDate': 'payment_date',
                'period': 'period',
                'recordDate': 'record_date',
                'unadjustedValue': 'unadjusted_value',
                'value': 'adjusted_value',
            }
            date_columns_list = [
                'date_stamp', 'declaration_date', 'payment_date', 'record_date']
            symbol_list = [
                (s.ticker, s.exchange.eod_code) for s in securities_list]
            feed = eod.BulkHistorical()
            try:
                data = feed.get_dividends(symbol_list, from_date, to_date)
            except Exception() as ex:
                logger.error('Failed to get dividend data.')
                raise ex
            # If no data then just return a simple empty pandas DataFrame.
            if data.empty:
                return pd.DataFrame
            #  Reset the index as we need to form here on treat the index as
            # column data.
            data.reset_index(inplace=True)
            # Extract by columns name and rename to a standard. This is also
            # then a check for expected columns.
            data = data[list(column_dict.keys())]
            data.rename(columns=column_dict, inplace=True)
            # Replace EODHistoricalData.com's exchange codes (the mic column)
            # with exchange MICs
            eod_to_mic_dict = dict([
                (s.exchange.eod_code, s.exchange.mic) for s in securities_list])
            data.replace({'mic': eod_to_mic_dict}, inplace=True)
            # Augment the ticker-mic with the matching ISIN code using a mapping
            # dictionary
            mic_ticker_to_isin_dict = dict([
                ((s.exchange.mic, s.ticker), s.isin) for s in securities_list])
            data['_key'] = data[['mic', 'ticker']
                                ].to_records(index=False).tolist()
            data['isin'] = data['_key'].map(mic_ticker_to_isin_dict)
            # These columns are expected by asset_base
            data.drop(columns=['_key', 'mic', 'ticker'], inplace=True)
            # Condition date
            for column in date_columns_list:
                data[column] = pd.to_datetime(data[column])
        else:
            raise Exception('Feed {} not implemented.'.format(feed))

        return data


class ForexHistory(_Feed):
    """Provide Forex historical data from data feeds.

    """

    _data_path = None
    _sub_path = None

    forex_list = [
        'USD', 'EUR', 'GBP', 'CAD', 'AUD', 'JPY', 'CHF', 'CNY', 'HKD',
        'NZD', 'SEK', 'KRW', 'SGD', 'NOK', 'MXN', 'INR', 'RUB', 'ZAR']

    def get_eod(
            self, forex_list, from_date=None, to_date=None, feed='EOD'):
        """ Get historical EOD for a specified list of securities.

        This method fetches the data from the specified feed.

        forex_list : list of .asset_base.Listed or child classes
            A list of securities that are listed and traded.
        from_date : datetime.date
            Inclusive start date of historical data. If not provided then the
            date is set to 1900-01-01.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.
        feed : str
            The data feed module to use:
                'EOD' - eod_historical_data

        """
        if forex_list is None:
            forex_list = self.forex_list

        if feed == 'EOD':
            column_dict = {
                'date': 'date_stamp',
                'ticker': 'ticker',
                'adjusted_close': 'adjusted_close',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'open': 'open',
                'volume': 'volume',
            }

            try:
                data = feed.get_forex(forex_list, from_date, to_date)
            except Exception as ex:
                logger.error('Failed to get Forex data.')
                raise ex
            # If no data then just return a simple empty pandas DataFrame.
            if data.empty:
                return pd.DataFrame
            #  Reset the index as we need to form here on treat the index as
            # column data.
            data.reset_index(inplace=True)
            # Extract by columns name and rename to a standard. This is also
            # then a check for expected columns.
            data = data[list(column_dict.keys())]
            data.rename(columns=column_dict, inplace=True)
            # Condition date
            data['date_stamp'] = pd.to_datetime(data['date_stamp'])
        else:
            raise Exception('Feed {} not implemented.'.format(feed))

        return data
