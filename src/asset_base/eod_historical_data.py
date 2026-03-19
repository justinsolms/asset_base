#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Get financial data from the https://eodhistoricaldata.com/ API.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the asset_base module.
The asset_base module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

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

Note
----
The eodhistoricaldata.com API is a paid service. To use this module you must
have an API token which you can get by subscribing to the service. Once you have
the token, set it as an environment variable with the name
`EOD_HISTORICAL_DATA_API_TOKEN` and the value of the token. The module will read
the token from the environment variable and use it in the API calls.

Warning
-------
Date fields are kept as they are received as date strings `YYYY-MM-DD`. They are
not converted to Python ``datetime.datetime`` objects. Thi is because this class
is only about collecting and assembling feed data. It is the responsibility of
the classes that use this class' data to converts date strings.


"""
import os
import asyncio
import aiohttp
import sys
import datetime
import random
import pandas as pd

from asyncio import TimeoutError

from collections import defaultdict

# Get module-named logger.
import logging
import os

logging.basicConfig(stream=sys.stderr)
logger = logging.getLogger(__name__)


date_index_name = "date"
eod_columns = ["open", "close", "high", "low", "adjusted_close", "volume"]
dividend_columns = [
    "declarationDate",
    "recordDate",
    "paymentDate",
    "period",
    "value",
    "unadjustedValue",
    "currency",
]
split_columns = [
    "split"
]


class APISessionManager:
    """Session manager for EOD historical data API.

    The target API is the EOD historical data API at
    https://eodhistoricaldata.com/. The API is a paid service and requires an
    API token which must be set as an environment variable with the name
    `EOD_HISTORICAL_DATA_API_TOKEN` and the value of the token. The module will
    read the token from the environment variable and use it in the API calls.

    The api session manager is an asynchronous context manager which manages the
    lifecycle of the aiohttp client session and connection pool. It also
    provides a `get` method which is used by the other classes to make API
    calls. The `get` method also implements mechanisms for handling
    timeouts and other typical errors which may arise in API calls with the goal
    of completing the API call successfully and returning the result.

    The results are returned as the original JSON response.

    """

    # API domain
    _DOMAIN = "eodhistoricaldata.com"
    # Limiting connection pool size
    _CONNECTION_LIMIT = 4

    # Client total timeout in seconds
    _TIMEOUT = 5 * 60

    # Retry policy for transient transport and service errors.
    _RETRY_ATTEMPTS = 4
    _RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
    _BACKOFF_BASE_SECONDS = 0.5
    _BACKOFF_MAX_SECONDS = 8.0

    def __init__(self) -> None:
        api_token = os.environ.get("EOD_HISTORICAL_DATA_API_TOKEN")
        if not api_token:
            raise EnvironmentError(
                "EOD_HISTORICAL_DATA_API_TOKEN environment variable is not set."
            )
        # Prepare the URL
        self.url = f"https://{self._DOMAIN}"
        # Default to JSON format at the request of the service provider. There
        # is an issue that CSV includes a last line with the total number of
        # bytes which causes pandas read problems. Use JSON for now.
        self.base_params = {"api_token": api_token, "fmt": "json"}

    async def __aenter__(self):
        # Get connector object
        self.conn = aiohttp.TCPConnector(limit=self._CONNECTION_LIMIT)
        # Specify timeouts - see StackOverflow (answer by glezo) url t.ly/VqKl
        session_timeout = aiohttp.ClientTimeout(
            total=None, sock_connect=self._TIMEOUT, sock_read=self._TIMEOUT
        )
        # Get session object for starting a session with
        # connector_owner=False: this class explicitly closes the connector in
        # __aexit__, so the session must not close it a second time on its own.
        self.session = aiohttp.ClientSession(
            connector=self.conn, connector_owner=False, timeout=session_timeout
        )

        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self.session.close()
        await self.conn.close()

    def _is_last_attempt(self, attempt_number):
        return attempt_number >= self._RETRY_ATTEMPTS

    def _calculate_retry_delay(self, attempt_number, response=None):
        # Honor server Retry-After headers for rate limiting/service pressure.
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after is not None:
                try:
                    retry_after_seconds = float(retry_after)
                    if retry_after_seconds >= 0:
                        return min(retry_after_seconds, self._BACKOFF_MAX_SECONDS)
                except ValueError:
                    # Ignore non-numeric Retry-After values.
                    pass

        # Exponential backoff with jitter reduces synchronized retry bursts.
        base_delay = min(
            self._BACKOFF_BASE_SECONDS * (2 ** (attempt_number - 1)),
            self._BACKOFF_MAX_SECONDS,
        )
        return base_delay + random.uniform(0, base_delay * 0.1)

    async def get(self, endpoint, params):
        """Get API response with retries for transient errors."""
        url = f"{self.url}{endpoint}"

        # Merge the base parameters with the specific parameters
        all_params = {**self.base_params, **params}

        # Try several times to get the response
        for attempt in range(1, self._RETRY_ATTEMPTS + 1):
            try:
                async with self.session.get(url, params=all_params) as response:
                    logger.info("Initiated: %s", response.url)

                    if response.status in self._RETRYABLE_STATUS_CODES:
                        text = await response.text()
                        status = response.status
                        response_url = response.url
                        msg = (
                            f"Retryable response status={status}: text={text}: "
                            f"url={response_url}"
                        )
                        if self._is_last_attempt(attempt):
                            logger.warning(
                                "Fail (retryable status retries exceeded): %s", msg
                            )
                            raise Exception(msg)

                        retry_delay = self._calculate_retry_delay(
                            attempt_number=attempt, response=response
                        )
                        logger.warning(
                            "Retry (%s/%s) after status %s in %.2fs: %s",
                            attempt,
                            self._RETRY_ATTEMPTS,
                            status,
                            retry_delay,
                            response_url,
                        )
                        await asyncio.sleep(retry_delay)
                        continue

                    if not response.ok:
                        text = await response.text()
                        status = response.status
                        response_url = response.url
                        msg = (
                            f"Failed response status={status}: text={text}: "
                            f"url={response_url}"
                        )
                        logger.warning(msg)
                        raise Exception(msg)

                    data = await response.json()
            except (
                TimeoutError,
                aiohttp.ClientConnectionError,
                aiohttp.ClientPayloadError,
                aiohttp.ServerDisconnectedError,
            ) as ex:
                if self._is_last_attempt(attempt):
                    msg = f"Fail (transient retries exceeded): {url}"
                    logger.warning(msg)
                    raise

                retry_delay = self._calculate_retry_delay(attempt_number=attempt)
                logger.warning(
                    "Retry (%s/%s) after transient error %s in %.2fs: %s",
                    attempt,
                    self._RETRY_ATTEMPTS,
                    type(ex).__name__,
                    retry_delay,
                    url,
                )
                await asyncio.sleep(retry_delay)
            else:
                logger.debug("Success: %s", response.url)
                # Convert any None values to NaN for pandas compatibility.
                data = [
                    {k: (v if v is not None else float("nan")) for k, v in row.items()}
                    for row in data
                ]
                return data

        # Defensive fallback: loop should either return data or raise.
        raise Exception(f"Failed API call after retries: {url}")


class Historical(APISessionManager):
    """Get EOD historical data sets."""

    _historical_eod = "/api/eod"
    _historical_forex = "/api/eod"
    _historical_index = "/api/eod"
    _historical_dividends = "/api/div"
    _historical_splits = "/api/splits"

    async def _get(self, path, exchange, ticker, from_date=None, to_date=None):
        """Generic getter, daily, EOD historical data table over a date range.

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
        path = "{}/{}.{}".format(path, ticker, exchange)

        # Substitute defaults for missing `form` and `to` dates
        if from_date is None:
            from_date = datetime.datetime.strptime("1900-01-01", "%Y-%m-%d")
        if to_date is None:
            to_date = datetime.datetime.today()

        # Get the API response
        params = {
            "from": from_date.strftime("%Y-%m-%d"),
            "to": to_date.strftime("%Y-%m-%d"),
            "period": "d",  # Default to daily sampling period
            "order": "a",  # Default to ascending order
        }
        table = await self.get(path, params=params)

        if table.empty:
            return table

        # Condition date, date-index and sort and check for duplicates
        table[date_index_name] = pd.to_datetime(table[date_index_name])
        table.set_index(date_index_name, verify_integrity=True, inplace=True)
        table.sort_index(inplace=True)

        return table

    async def get_eod(self, exchange, ticker, from_date=None, to_date=None):
        """Get daily, EOD historical over a date range.

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
        table = await self._get(
            self._historical_eod,
            exchange, ticker,
            from_date=from_date, to_date=to_date
        )
        table = table[eod_columns]

        return table

    async def get_dividends(self, exchange, ticker, from_date=None, to_date=None):
        """Get daily, EOD historical dividends over a date range.

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
        table = await self._get(
            self._historical_dividends,
            exchange, ticker,
            from_date=from_date, to_date=to_date,
        )

        # Select only the expected dividend columns
        table = table[dividend_columns]

        # Normalise dtypes to match test and downstream expectations:
        # - date-like/reference fields stay as generic objects (string/ISO date)
        # - numeric fields are floats
        for col in ["declarationDate", "recordDate", "paymentDate", "period", "currency"]:
            if col in table.columns:
                table[col] = table[col].astype("object")
        for col in ["value", "unadjustedValue"]:
            if col in table.columns:
                table[col] = table[col].astype("float64")

        return table

    async def get_splits(self, exchange, ticker, from_date=None, to_date=None):
        """Get daily, EOD historical splits over a date range.

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
        table = await self._get(
            self._historical_splits,
            exchange, ticker,
            from_date=from_date, to_date=to_date,
        )
        table = table[split_columns]

        return table

    async def get_forex(self, ticker, from_date=None, to_date=None):
        """Get daily, EOD historial forex (USD based) over a date range.

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
        table = await self._get(
            self._historical_forex,
            "FOREX", ticker,
            from_date=from_date, to_date=to_date,
        )
        table = table[eod_columns]

        return table


class Bulk(APISessionManager):
    """Get Bulk data sets."""

    _bulk_eod = "/api/eod-bulk-last-day"

    async def _get(self, exchange, date=None, symbols=None, type=None):
        """Generic getter, bulk EOD for the exchange for a particular day.

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
        path = "{}/{}".format(self._bulk_eod, exchange)

        if date is None:
            # Find yesterday's date.
            date = datetime.date.today() - datetime.timedelta(days=1)

        if symbols is not None:
            # Create comma separated list after prepending with a `dot` and the
            # exchange symbol.
            symbols = ",".join(["{}.{}".format(sym, exchange) for sym in symbols])

        # Get the API response

        params = dict(
            date=date.strftime("%Y-%m-%d"),
            fmt="json",  # Default to CSV table. See NOTE in get!
            order="a",  # Default to ascending order
        )
        if type is not None:
            # The type=None is not liked by aiohttp.ClientSession() instance
            params["type"] = type
        if symbols is not None:
            # The symbols=None is not liked by aiohttp.ClientSession() instance
            params["symbols"] = symbols
        table = await self.get(path, params=params)

        if table.empty:
            return table

        # Fix the exchange column name
        if "exchange_short_name" in table.columns:
            table.rename(columns={"exchange_short_name": "exchange"}, inplace=True)
        # Condition date
        table[date_index_name] = pd.to_datetime(table[date_index_name])
        table.rename(columns={"code": "ticker"}, inplace=True)  # Fix API names
        table.set_index([date_index_name, "ticker", "exchange"], inplace=True)
        table.sort_index(inplace=True)  # MultiIndex must be sorted for slicing.

        return table

    async def get_eod(self, exchange, date=None, symbols=None):
        """Get bulk EOD price and volume for the exchange on a date.

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
        """Get bulk EOD dividends for the exchange on a date.

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
        table = await self._get(exchange, date=date, symbols=None, type="dividends")
        # Handle an anomaly in the EOD feed JSON API results.
        table["dividend"] = table["dividend"].astype(float)
        table["unadjustedValue"] = table["unadjustedValue"].astype(float)

        return table

    async def get_splits(self, exchange, date=None):
        """Get bulk EOD splits for the exchange on a date.

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
        table = await self._get(exchange, date=date, symbols=None, type="splits")

        return table


class Fundamentals(APISessionManager):
    """Get fundamental data API for stocks, ETFs, Mutual Funds, Indices."""

    # TODO: Add fundamental public methods
    _fundamentals = "/api/fundamentals"

    async def _get(self, exchange, ticker):
        """Get fundamental data API for stocks, ETFs, Mutual Funds, Indices.

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
        path = "{}/{}.{}".format(self._fundamentals, ticker, exchange)

        # Get the API response
        params = dict(
            fmt="json",  # Default to CSV table. See NOTE in get!
            period="d",  # Default to daily sampling period
            order="a",  # Default to ascending order
        )
        table = await self.get(path, params=params)

        return table


class Exchanges(object):
    """Get exchanges (and list of indices) data."""

    def get_exchanges(self):
        """Get the full list of supported exchanges."""
        # Path must append ticker and short exchange code
        _exchanges = "/api/exchanges-list"
        path = "{}".format(_exchanges)

        # Get the API response
        params = dict(
            fmt="json",  # Default to CSV table. See NOTE in get!
            period="d",  # Default to daily sampling period
            order="a",  # Default to ascending order
        )

        async def _get(path, params):
            async with APISessionManager() as exchanges:
                task = await exchanges.get(path, params=params)
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
        _exchange_symbol_list = "/api/exchange-symbol-list"
        path = "{}/{}".format(_exchange_symbol_list, exchange)

        # Get the API response
        params = dict(
            fmt="json",  # Default to CSV table. See NOTE in get!
            period="d",  # Default to daily sampling period
            order="a",  # Default to ascending order
        )

        async def _get(path, params):
            async with APISessionManager() as exchanges:
                task = await exchanges.get(path, params=params)
            return task

        table = asyncio.run(_get(path, params))

        return table

    def get_indices(self):
        """Get a table of supported indices."""
        return self.get_exchange_symbols("INDX")


class MultiHistorical(object):
    """Get multiple histories across exchanges, securities and date ranges.

    This class' public methods take a list of `(exchange, ticker)` tuples and
    generate a single call per `(exchange, ticker)` tuple from the appropriate
    `get` method. The `get` method chosen depends on the type of data required
    and the date range.

    The class then gathers multiple API call results (single column
    ``pandas.DataFrame`` objects) into a multi-column data table
    (``pandas.DataFrame``) which is returned.

    """

    async def _get_eod(self, path, symbol_list):
        """Get historical data for a list of securities.

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
                    historical._get(path, exchange, ticker, from_date, to_date)
                )
            result_list = await asyncio.gather(*tasks, return_exceptions=True)
            # Add ticker and exchange code to each table in the list. The API
            # does not return this data so it must be constructed and attached.
            # Skip over exceptions.
            table_list = list()
            for i, result in enumerate(zip(symbol_list, result_list)):
                symbol, unknown = result
                ticker, exchange, from_date, to_date = symbol
                # Check dates, guard against None's
                assert from_date is None or isinstance(from_date, datetime.date), (
                    "Expected symbol_list" "s from_date type as datetime.date."
                )
                assert to_date is None or isinstance(to_date, datetime.date), (
                    "Expected symbol_list" "s to_date type as datetime.date."
                )
                if isinstance(unknown, Exception):
                    exception = unknown
                    logger.warning(
                        "Exception %s for symbol %s.%s", exception, ticker, exchange
                    )
                else:
                    # Process table and add to list
                    table = unknown
                    table["ticker"] = ticker
                    table["exchange"] = exchange
                    table_list.append(table)
        # Eliminate empty tables in the table list as these inadvertently erase
        # the `date` index name. This may be a `pandas` bug.
        table_list = [table for table in table_list if not table.empty]
        # Case management after elimination leaving zero, one or several tables
        if len(table_list) == 0:
            # Return an empty table
            return pd.DataFrame([])
        # Concatenate all tables
        table = pd.concat(table_list, axis="index")
        # Set up full index by appending ticker and exchange to the date index
        table.set_index(
            ["ticker", "exchange"], verify_integrity=True, append=True, inplace=True
        )
        # MultiIndex must be sorted for causal slicing.
        table.sort_index(inplace=True)

        return table

    async def _get_bulk(self, symbol_list, from_date, to_date=None, type=None):
        """Get bulk historical data for a range of dates.

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
        dates = [
            from_date + datetime.timedelta(days=x)
            for x in range((to_date - from_date).days + 1)
        ]

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
                    tasks.append(bulk._get(exchange, date, ticker_list, type))
            table_list = await asyncio.gather(*tasks)

        # Contrary to _get_eod the API does return date, exchange and ticker
        # data so it not not be constructed and attached. Combine tables in to
        # one large table
        table = pd.concat(table_list, axis="index")
        table.sort_index(inplace=True)  # MultiIndex must be sorted for slicing.
        # Duplicates are caused by holidays. Querying the API on the evenings of
        # Friday (which did return a non-trivial result), and Saturday and
        # Sunday would produce 3 identical entries, all dated Friday. So we need
        # to drop these.
        table.drop_duplicates(inplace=True)

        return table

    def get_eod(self, symbol_list):
        """Get historical EOD for a list of securities.

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
        # Use EOD API
        table = asyncio.run(self._get_eod(Historical._historical_eod, symbol_list))

        if table.empty:
            # Produce an empty DataFrame that will pass empty tests downstream
            table = pd.DataFrame()
        else:
            # The security and date info is in the index
            table = table[eod_columns]

        return table

    def get_dividends(self, symbol_list):
        """Get historical dividends for a list of securities.

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
        # Use EOD API
        table = asyncio.run(
            self._get_eod(Historical._historical_dividends, symbol_list)
        )

        if table.empty:
            # Produce an empty DataFrame that will pass empty tests downstream
            table = pd.DataFrame()
        else:
            # The security and date info is in the index
            table = table[dividend_columns]

        # Some DataFrames values are sometimes None - convert only these to
        # float NaNs



        return table

    def get_splits(self, symbol_list):
        """Get historical splits for a list of securities.

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
        # Use EOD API
        table = asyncio.run(
            self._get_eod(Historical._historical_splits, symbol_list)
        )

        if table.empty:
            # Produce an empty DataFrame that will pass empty tests downstream
            table = pd.DataFrame()
        else:
            # The security and date info is in the index
            table = table[split_columns]

        return table

    def get_forex(self, forex_list):
        """Get historical forex for a list of rates.

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
        # Re-construct a symbol list form the forex ticker list as (`exchange`,
        # `ticker`) pairs (as in the ``get_eod`` method) with the `exchange`
        # part set to "FOREX". In other words, insert FOREX in the right
        # position.
        symbol_list = [
            (ticker, "FOREX", from_date, to_date)
            for ticker, from_date, to_date in forex_list
        ]

        # Use EOD API
        table = asyncio.run(self._get_eod(Historical._historical_forex, symbol_list))

        if table.empty:
            # Produce an empty DataFrame that will pass empty tests downstream
            table = pd.DataFrame()
        else:
            # The security and date info is in the index
            table = table[eod_columns]
            # As the exchange suffix is always 'FOREX' it is unnecessary.
            table = table.droplevel(level="exchange")

        return table

    def get_index(self, index_list):
        """Get historical forex for a list of rates.

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
        # Re-construct a symbol list form the forex ticker list as (`exchange`,
        # `ticker`) pairs (as in the ``get_eod`` method) with the `exchange`
        # part set to "FOREX". In other words, insert FOREX in the right
        # position.
        symbol_list = [
            (ticker, "INDX", from_date, to_date)
            for ticker, from_date, to_date in index_list
        ]

        # Use EOD API
        table = asyncio.run(self._get_eod(Historical._historical_forex, symbol_list))

        if table.empty:
            # Produce an empty DataFrame that will pass empty tests downstream
            table = pd.DataFrame()
        else:
            # The security and date info is in the index
            table = table[eod_columns]
            # As the exchange suffix is always 'INDX' it is unnecessary.
            table = table.droplevel(level="exchange")

        return table
