#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Get financial data from the https://eodhistoricaldata.com/ API.

This module has different levels of abstraction classes:
1. Direct API query, response and result checking.
2. Generic classes for EOD and Bulk and Fundamental data.
3. High level data retrieval and management class.

End-of-day (EOD) data is historic daily data as of the end of the trading day.
Bulk data is EOD data for a single day across all, or a large set of
securities, for a single exchange code.

All tabular data are returned as a ``pandas.DataFrame``.

The module uses the Python ``requests`` package. This also means we can use
``datetime.datetime`` interchangeably date ``str`` objects for dates.

Warning
-------
Date fields are kept as they are received as date strings `YYYY-MM-DD`. They are
not converted to Python ``datetime.datetime`` objects. Thi is because this class
is only about collecting and assembling feed data. It is the responsibility of
the classes that use this class' data to converts date strings.


"""
import asyncio
import aiohttp
import sys
import datetime
import pandas as pd

from asyncio import TimeoutError

from collections import defaultdict

# Get module-named logger.
import logging
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger(__name__)


class _API(aiohttp.ClientSession):
    """ Direct API query, response and result checking.

    Parameters
    ----------
    asset_base : fundmanage.asset_base.EntityBase
        An ``asset_base`` database manager with a session to the database.

    """
    # Feed
    _domain = 'eodhistoricaldata.com'
    _api_token = '60802039419943.54316578'

    async def _get_retries(self, path, params):
        """ Get and check a response form the API.

        Note
        ----
        Retries by exception handling are implemented here.

        Parameters
        ----------
        path : str
            The service path in the domain. Must append the instrument code at
            the end.
        params : dict
            Additional (key: value) pair parameters such as date range. The API
            token _must not_ be included.

        Returns
        -------
        csv
            Response
        """
        # Prepare the URL
        url = 'https://{}{}'.format(self._domain, path)

        # Add API token to params
        params['api_token'] = self._api_token
        # Default to JSON format at the request of the service provider. There
        # is an issue that CSV includes a last line with the total number of
        # bytes which causes pandas read problems. Use JSON for now.
        params['fmt'] = 'json'

        # The API requires `from` and `to` as arguments.
        if 'from_date' in params:
            params['from'] = params.pop('from_date')
        if 'to_date' in params:
            params['to'] = params.pop('to_date')

        # Retry loop
        retry_list = [1, 2, 'last']
        for retry in retry_list:
            try:
                response = await self.get(url, params=params, timeout=20)
            except TimeoutError as ex:
                # Test for retries
                if retry == 'last':
                    logger.info('Timeout (try-%s): %s', retry, url)
                    raise ex
                else:
                    # Go around for a retry
                    logger.info('Timeout (try-%s): %s', retry, url)
                    continue
            else:
                logger.info('Initiated (try-%s): %s', retry, response.url)
                if response.ok:
                    json = await response.json()
                    # Success - break out of retry loop
                    logger.info('Success: %s', response.url)
                    break
                else:
                    text = await response.text()
                    status = response.status
                    url = response.url
                    raise Exception(
                        'Failed response %s:%s for %s', status, text, url)

        # JSON to DataFrame
        table = pd.DataFrame(json)

        return table


class Historical(_API):
    """ Get EOD historical data sets. """

    _historical_eod = '/api/eod'
    _historical_forex = '/api/eod'
    _historical_index = '/api/eod'
    _historical_dividends = '/api/div'

    async def _get(self, path, exchange, ticker, from_date=None, to_date=None):
        """ Generic getter, daily, EOD historical data table over a date range.

        This is a common history `getter method used to get eod, dividend and
        forex history as specified by the ``path`` argument.

        Parameters
        ----------
        path : str
            The domain's specific API service path, example: "/api/eod"
            for EOD prices
        exchange : str
            Short exchange code for the listed security.
        ticker : str
            Exchange security ticker code
        from_date : datetime.date, optional
            Inclusive start date of historical data. If not provided then the
            date is set to 1900-01-01.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.

        Returns
        -------
        pandas.DataFrame
            The daily, EOD historical time-series.
        """
        # Path must append ticker and short exchange code
        path = '{}/{}.{}'.format(path, ticker, exchange)

        # Substitute defaults for missing `form` and `to` dates
        if from_date is None:
            from_date = datetime.datetime.strptime('1900-01-01', '%Y-%m-%d')
        if to_date is None:
            to_date = datetime.datetime.today()

        # Get the API response
        params = dict(
            from_date=from_date.strftime('%Y-%m-%d'),
            to_date=to_date.strftime('%Y-%m-%d'),
            period='d',  # Default to daily sampling period
            order='a',  # Default to ascending order
        )
        table = await self._get_retries(path, params=params)

        if table.empty:
            return table

        # Condition date
        table['date'] = pd.to_datetime(table['date'])
        table.set_index('date', verify_integrity=True, inplace=True)
        table.sort_index(inplace=True)

        return table

    async def get_eod(self, exchange, ticker, from_date=None, to_date=None):
        """ Get daily, EOD historical over a date range.

        Parameters
        ----------
        exchange : str
            Short exchange code for the listed security.
        ticker : str
            Exchange security ticker code
        from_date : datetime.date, optional
            Inclusive start date of historical data. If not provided then the
            date is set to 1900-01-01.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.

        Returns
        -------
        pandas.DataFrame
            The daily, EOD historical time-series.
        """
        return await self._get(
            self._historical_eod, exchange, ticker,
            from_date=from_date, to_date=to_date)

    async def get_dividends(
            self, exchange, ticker, from_date=None, to_date=None):
        """ Get daily, EOD historical dividends over a date range.

        Parameters
        ----------
        exchange : str
            Short exchange code for the listed security.
        ticker : str
            Exchange security ticker code
        from_date : datetime.date, optional
            Inclusive start date of historical data. If not provided then the
            date is set to 1900-01-01.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.

        Returns
        -------
        pandas.DataFrame
            The daily, EOD historical time-series.
        """
        return await self._get(
            self._historical_dividends, exchange, ticker,
            from_date=from_date, to_date=to_date)

    async def get_forex(self, ticker, from_date=None, to_date=None):
        """ Get daily, EOD historial forex (USD based) over a date range.

        Parameters
        ----------
        ticker : str
            Exchange security ticker code
        from_date : datetime.date, optional
            Inclusive start date of historical data. If not provided then the
            date is set to 1900-01-01.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.

        Returns
        -------
        pandas.DataFrame
            The daily, EOD historical time-series.
        """
        return await self._get(
            self._historical_forex, 'FOREX', ticker,
            from_date=from_date, to_date=to_date)


class Bulk(_API):
    """ Get Bulk data sets. """

    _bulk_eod = '/api/eod-bulk-last-day'

    async def _get(self, exchange, date=None, symbols=None, type=None):
        """ Generic getter, bulk EOD for the exchange for a particular day.

        This is a common bulk `get` method used to get eod, dividend and
        splits across an exchange on a particular day.


        Note
        ----
        Empty data is returned on exchange holidays (weekends and other).

        Parameters
        ----------
        exchange : str
            Short exchange code for the listed security.
        date : datetime.date, optional
            The single date of the data. If none provided then the date is
            yesterday which will yield yesterday's eod as today is assumed to
            not have an eod.
        symbols : list, optional
            A list of exchange listed security ticker symbols. If none provided
            then all tickers on the exchange are provided.
        type : None, 'dividends', or 'splits'
            None (default) for end of day data, or dividends or splits

        """
        # Path must append short exchange code
        path = '{}/{}'.format(self._bulk_eod, exchange)

        if date is None:
            # Find yesterday's date.
            date = datetime.date.today() - datetime.timedelta(days=1)

        if symbols is not None:
            # Create comma separated list after prepending with a `dot` and the
            # exchange symbol.
            symbols = ','.join(
                ['{}.{}'.format(sym, exchange) for sym in symbols]
            )

        # Get the API response

        params = dict(
            date=date.strftime('%Y-%m-%d'),
            fmt='json',  # Default to CSV table. See NOTE in _get_retries!
            order='a',  # Default to ascending order
        )
        if type is not None:
            # The type=None is not liked by aiohttp.ClientSession() instance
            params['type'] = type
        if symbols is not None:
            # The symbols=None is not liked by aiohttp.ClientSession() instance
            params['symbols'] = symbols
        table = await self._get_retries(path, params=params)

        if table.empty:
            return table

        # Fix the exchange column name
        if 'exchange_short_name' in table.columns:
            table.rename(columns={'exchange_short_name': 'exchange'},
                         inplace=True)
        # Condition date
        table['date'] = pd.to_datetime(table['date'])
        table.rename(columns={'code': 'ticker'}, inplace=True)  # Fix API names
        table.set_index(['date', 'ticker', 'exchange'], inplace=True)
        table.sort_index(inplace=True)  # MultiIndex must be sorted for slicing.

        return table

    async def get_eod(self, exchange, date=None, symbols=None):
        """ Get bulk EOD price and volume for the exchange on a date.

        Parameters
        ----------
        exchange : str
            Short exchange code for the listed security.
        date : datetime.date, optional
            The single date of the data. If none provided then the date is
            yesterday.
        symbols : list, optional
            A list of exchange listed security ticker symbols. If none provided
            then all tickers on the exchange are provided.

        """
        return await self._get(exchange, date=date, symbols=symbols)

    async def get_dividends(self, exchange, date=None):
        """ Get bulk EOD dividends for the exchange on a date.

        Note
        ----
        If there were no dividends on the date then an empty pd.DataFrame is
        returned.

        Note
        ----
        Empty data is returned on exchange holidays (weekends and other).

        Parameters
        ----------
        exchange : str
            Short exchange code for the listed security.
        date : datetime.date, optional
            The single date of the data. If none provided then the date is
            yesterday.

        """
        table = await self._get(
            exchange, date=date, symbols=None, type='dividends')
        # Handle an anomaly in the EOD feed JSON API results.
        table['dividend'] = table['dividend'].astype(float)
        table['unadjustedValue'] = table['unadjustedValue'].astype(float)

        return table

    async def get_splits(self, exchange, date=None):
        """ Get bulk EOD splits for the exchange on a date.

        Note
        ----
        If there were no dividends on the date then an empty pd.DataFrame is
        returned.

        Note
        ----
        Empty data is returned on exchange holidays (weekends and other).

        Parameters
        ----------
        exchange : str
            Short exchange code for the listed security.
        date : datetime.date, optional
            The single date of the data. If none provided then the date is
            yesterday.

        """
        table = await self._get(
            exchange, date=date, symbols=None, type='splits')

        return table


class Fundamentals(_API):
    """ Get fundamental data API for stocks, ETFs, Mutual Funds, Indices. """

    # TODO: Add fundamental public methods
    _fundamentals = '/api/fundamentals'

    async def _get(self, exchange, ticker):
        """ Get fundamental data API for stocks, ETFs, Mutual Funds, Indices.

        Note
        ----
        This is currently not subscribed to as they do not carry JSE ETF
        fundamental data.

        Parameters
        ----------
        exchange : str
            Short exchange code for the listed security.
        ticker : str
            Exchange security ticker code

        """
        # Path must append ticker and short exchange code
        path = '{}/{}.{}'.format(self._fundamentals, ticker, exchange)

        # Get the API response
        params = dict(
            fmt='json',  # Default to CSV table. See NOTE in _get_retries!
            period='d',  # Default to daily sampling period
            order='a',  # Default to ascending order
        )
        table = await self._get_retries(path, params=params)

        return table


class Exchanges(object):
    """Get exchanges (and list of indices) data."""

    def get_exchanges(self):
        """Get the full list of supported exchanges."""
        # Path must append ticker and short exchange code
        _exchanges = '/api/exchanges-list'
        path = '{}'.format(_exchanges)

        # Get the API response
        params = dict(
            fmt='json',  # Default to CSV table. See NOTE in _get_retries!
            period='d',  # Default to daily sampling period
            order='a',  # Default to ascending order
        )

        async def _get(path, params):
            async with _API() as exchanges:
                task = await exchanges._get_retries(path, params=params)
            return task

        table = asyncio.run(_get(path, params))

        return table

    def get_exchange_symbols(self, exchange):
        """Get the full table of symbols (tickers) on the exchange.

        Parameters
        ----------
        exchange : str
            Short exchange code.

        """
        # Path must append ticker and short exchange code
        _exchange_symbol_list = '/api/exchange-symbol-list'
        path = '{}/{}'.format(_exchange_symbol_list, exchange)

        # Get the API response
        params = dict(
            fmt='json',  # Default to CSV table. See NOTE in _get_retries!
            period='d',  # Default to daily sampling period
            order='a',  # Default to ascending order
        )

        async def _get(path, params):
            async with _API() as exchanges:
                task = await exchanges._get_retries(path, params=params)
            return task

        table = asyncio.run(_get(path, params))

        return table

    def get_indices(self):
        """Get a table of supported indices."""
        return self.get_exchange_symbols('INDX')


class MultiHistorical(object):
    """ Get multiple histories across exchanges, securities and date ranges.

    This class' public methods take a list of `(exchange, ticker)` tuples and
    generate a single call per `(exchange, ticker)` tuple from the appropriate
    `get` method. The `get` method chosen depends on the type of data required
    and the date range.

    The class then gathers multiple API call results (single column
    ``pandas.DataFrame`` objects) into a multi-column data table
    (``pandas.DataFrame``) which is returned.

    """
    async def _get_eod(self, path, symbol_list):
        """ Get historical data for a list of securities.

        This uses the EOD history API service (class ``Historical``) which means
        histories for all securities in the ``symbol_list` argument are
        retrieved as one security history per API call.

        This method combines multiple ``Historical`` class getter method call
        results, one for each `(exchange, ticker)` tuple in the ``symbol_list``
        argument, into a single ``pandas.DataFrame``.

        Parameters
        ----------
        path : str
            The domain's specific API service path, example: "/api/eod"
            for EOD prices
        symbol_list : list of tuples
            A list of ticker-exchange and date range list. As an example, if
            Apple Inc (ticker AAPL, exchange US). was required between
            2021-01-01 and 2022-01-01 then it's symbol tuple would be: `('AAPL',
            'US', datetime.date(2021, 1, 1), datetime.date(2022, 1, 1))`. Note
            that the date must be `datetime.date` or an exception shall be
            thrown.

        Note
        ----
        All date strings must be ISO date strings `yyyy-mm-dd` or `%Y-%m-%d`.

        """
        # Each security has its own from date.
        tasks = list()
        async with Historical() as historical:
            for ticker, exchange, from_date, to_date in symbol_list:
                # Call historical EOD
                tasks.append(
                    historical._get(
                        path, exchange, ticker, from_date, to_date))
            result_list = await asyncio.gather(*tasks, return_exceptions=True)
            # Add ticker and exchange code to each table in the list. The API
            # does not return this data so it must be constructed and attached.
            # Skip over exceptions.
            table_list = list()
            for i, result in enumerate(zip(symbol_list, result_list)):
                symbol, unknown = result
                ticker, exchange, from_date, to_date = symbol
                # Check dates, guard against None's
                assert from_date is None or isinstance(from_date, datetime.date),\
                    'Expected symbol_list''s from_date type as datetime.date.'
                assert to_date is None or isinstance(to_date, datetime.date),\
                    'Expected symbol_list''s to_date type as datetime.date.'
                if isinstance(unknown, Exception):
                    exception = unknown
                    logger.warning(
                        'Exception %s for symbol %s.%s',
                        exception, ticker, exchange)
                else:
                    # Process table and add to list
                    table = unknown
                    table['ticker'] = ticker
                    table['exchange'] = exchange
                    table_list.append(table)
        # Eliminate empty tables in the table list as these inadvertently erase
        # the `date` index name. This may be a `pandas` bug.
        table_list = [table for table in table_list if not table.empty]
        # Case management after elimination leaving zero, one or several tables
        if len(table_list) == 0:
            # Return an empty table
            return pd.DataFrame([])
        # Concatenate all tables
        table = pd.concat(table_list, axis='index')
        # Set up full index by appending ticker and exchange to the date index
        table.set_index(
            ['ticker', 'exchange'],
            verify_integrity=True, append=True, inplace=True)
        # MultiIndex must be sorted for causal slicing.
        table.sort_index(inplace=True)

        return table

    async def _get_bulk(
            self, symbol_list, from_date, to_date=None, type=None):
        """ Get bulk historical data for a range of dates.

        This uses the Bulk history API service (class Bulk) which means
        histories for all securities in the ``symbol_list` argument are
        retrieved for one exchange and one day per API call.

        This method combines multiple ``Historical`` class getter method call
        results, one for each `(exchange, ticker)` tuple in the ``symbol_list``
        argument, into a single ``pandas.DataFrame``.

        Parameters
        ----------
        symbol_list : list of tuples
            A list of ticker-exchange symbol_list. As an example Apple Inc. would be
            `AAPL.US` and it's symbol tuple would be `('AAPL', 'US')`, where
            `AAPL` is the exchange ticker and `US` is the exchange code
            (actually EOD code for all US exchanges).
        from_date : datetime.date
            Inclusive start date of historical data. If not provided then the
            date is set to 1900-01-01.
        to_date : datetime.date, optional
            Inclusive end date of historical data. If not provide then the date
            is set to today.
        type : None, 'dividends`, or 'splits'
            None (default) for end of day data, or dividends or splits

        """
        # Generate a date series between from_date and to_date (inclusive).
        dates = [from_date + datetime.timedelta(days=x)
                 for x in range((to_date - from_date).days + 1)]

        # Create a dict of exchanges keys with each item a list of the
        # securities specified (by symbols_list) in that exchange.
        exchange_dict = defaultdict(list)
        for ticker, exchange in symbol_list:
            exchange_dict[exchange].append(ticker)

        # Fetch securities across all exchange.
        tasks = list()
        async with Bulk() as bulk:
            for exchange, ticker_list in exchange_dict.items():
                for date in dates:
                    tasks.append(
                        bulk._get(
                            exchange, date, ticker_list, type)
                    )
            table_list = await asyncio.gather(*tasks)

        # Contrary to _get_eod the API does return date, exchange and ticker
        # data so it not not be constructed and attached. Combine tables in to
        # one large table
        table = pd.concat(table_list, axis='index')
        table.sort_index(inplace=True)  # MultiIndex must be sorted for slicing.
        # Duplicates are caused by holidays. Querying the API on the evenings of
        # Friday (which did return a non-trivial result), and Saturday and
        # Sunday would produce 3 identical entries, all dated Friday. So we need
        # to drop these.
        table.drop_duplicates(inplace=True)

        return table

    def get_eod(self, symbol_list):
        """ Get historical EOD for a list of securities.

        This method switches between EOD and Bulk feeds (classes Historical and
        Bulk) depending on the date range. This is to minimize time in the API
        calls.

        symbol_list : list of tuples
            A list of ticker-exchange and date range list. As an example, if
            Apple Inc (ticker AAPL, exchange US). was required between
            2021-01-01 and 2022-01-01 then it's symbol tuple would be: `('AAPL',
            'US', datetime.date(2021, 1, 1), datetime.date(2022, 1, 1))`. Note
            that the date must be `datetime.date` or an exception shall be
            thrown.

        """
        columns_names = [
            'adjusted_close', 'close', 'high', 'low', 'open', 'volume']

        # Use EOD API
        table = asyncio.run(
            self._get_eod(Historical._historical_eod, symbol_list))
        if table.empty:
            # Produce an empty DataFrame that will pass empty tests downstream
            table = pd.DataFrame()
        else:
            # The security and date info is in the index
            table = table[columns_names]

        return table

    def get_dividends(self, symbol_list):
        """ Get historical dividends for a list of securities.

        This method uses only the EOD (class Historical) due to incorrect Bulk
        API call behaviour such as not returning the ``value`` filed and not
        restricting to only the specified tickers or symbols.

        symbol_list : list of tuples
            A list of ticker-exchange and date range list. As an example, if
            Apple Inc (ticker AAPL, exchange US). was required between
            2021-01-01 and 2022-01-01 then it's symbol tuple would be: `('AAPL',
            'US', datetime.date(2021, 1, 1), datetime.date(2022, 1, 1))`. Note
            that the date must be `datetime.date` or an exception shall be
            thrown

        """
        columns_names = [
            'currency', 'declarationDate', 'paymentDate', 'period',
            'recordDate', 'unadjustedValue', 'value']

        # Use EOD API
        table = asyncio.run(
            self._get_eod(Historical._historical_dividends, symbol_list))
        if table.empty:
            # Produce an empty DataFrame that will pass empty tests downstream
            table = pd.DataFrame()
        else:
            # The security and date info is in the index
            table = table[columns_names]

        return table

    def get_forex(self, forex_list):
        """ Get historical forex for a list of rates.

        This method uses only the EOD (class Historical) due to incorrect Bulk
        API call behaviour such as not returning the ``value`` filed and not
        restricting to only the specified tickers or symbols.

        symbol_list : list of tuples
            A list of forex tickers and date range list. As an example, if USD
            to ZAR (ticker USDZAR) was required between 2021-01-01 and
            2022-01-01 then it's symbol tuple would be: `('USDZAR',
            datetime.date(2021, 1, 1), datetime.date(2022, 1, 1))`. Note that
            the date must be `datetime.date` or an exception shall be thrown

        """
        columns_names = [
            'adjusted_close', 'close', 'high', 'low', 'open', 'volume']

        # Re-construct a symbol list form the forex ticker list as (`exchange`,
        # `ticker`) pairs (as in the ``get_eod`` method) with the `exchange`
        # part set to "FOREX". In other words, insert FOREX in the right
        # position.
        symbol_list = [
            (ticker, 'FOREX', from_date, to_date
             ) for ticker, from_date, to_date in forex_list]

        # Use EOD API
        table = asyncio.run(
            self._get_eod(Historical._historical_forex, symbol_list))
        # As the exchange is always 'FOREX' it is unnecessary.
        table = table.droplevel(level='exchange')

        if table.empty:
            # Produce an empty DataFrame that will pass empty tests downstream
            table = pd.DataFrame()
        else:
            # The security and date info is in the index
            table = table[columns_names]

        return table

    def get_index(self, index_list):
        """ Get historical forex for a list of rates.

        This method uses only the EOD (class Historical) due to incorrect Bulk
        API call behaviour such as not returning the ``value`` filed and not
        restricting to only the specified tickers or symbols.

        symbol_list : list of tuples
            A list of index tickers and date range list. As an example, if the
            index ASX (FTSE All Share Index) was required between 2021-01-01 and
            2022-01-01 then it's symbol tuple would be: `('ASX',
            datetime.date(2021, 1, 1), datetime.date(2022, 1, 1))`. Note that
            the date must be `datetime.date` or an exception shall be thrown

        """
        columns_names = [
            'adjusted_close', 'close', 'high', 'low', 'open', 'volume']

        # Re-construct a symbol list form the forex ticker list as (`exchange`,
        # `ticker`) pairs (as in the ``get_eod`` method) with the `exchange`
        # part set to "FOREX". In other words, insert FOREX in the right
        # position.
        symbol_list = [
            (ticker, 'INDX', from_date, to_date
             ) for ticker, from_date, to_date in index_list]

        # Use EOD API
        table = asyncio.run(
            self._get_eod(Historical._historical_forex, symbol_list))
        # As the exchange is always 'FOREX' it is unnecessary.
        table = table.droplevel(level='exchange')

        if table.empty:
            # Produce an empty DataFrame that will pass empty tests downstream
            table = pd.DataFrame()
        else:
            # The security and date info is in the index
            table = table[columns_names]

        return table

