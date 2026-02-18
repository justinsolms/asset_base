#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Unit tests for financial_data module."""

import datetime
import unittest
from unittest.mock import patch

import pandas as pd

from src.asset_base.financial_data import Dump, History, MetaData, Static


class DummyExchange:
	def __init__(self, eod_code, mic):
		self.eod_code = eod_code
		self.mic = mic


class DummyAsset:
	def __init__(self, ticker, exchange, isin, last_eod=None, last_dividend=None, last_split=None):
		self.ticker = ticker
		self.exchange = exchange
		self.isin = isin
		self._last_eod = last_eod
		self._last_dividend = last_dividend
		self._last_split = last_split

	def get_last_eod_date(self):
		return self._last_eod

	def get_last_dividend_date(self):
		return self._last_dividend

	def get_last_split_date(self):
		return self._last_split


class DummyForex:
	def __init__(self, ticker, last_eod=None):
		self.ticker = ticker
		self._last_eod = last_eod

	def get_last_eod_date(self):
		return self._last_eod


class DummyIndex:
	def __init__(self, ticker, last_eod=None):
		self.ticker = ticker
		self._last_eod = last_eod

	def get_last_eod_date(self):
		return self._last_eod


def _make_eod_df(date_str="2020-01-02", exchange="US", tickers=None, exchanges=None):
	if tickers is None:
		tickers = ["AAA", "BBB"]
	if exchanges is None:
		exchanges = [exchange] * len(tickers)
	rows = []
	for ticker, ex_code in zip(tickers, exchanges):
		rows.append(
			{
				"date": pd.Timestamp(date_str),
				"ticker": ticker,
				"exchange": ex_code,
				"adjusted_close": 100.0,
				"close": 101.0,
				"high": 102.0,
				"low": 99.0,
				"open": 100.5,
				"volume": 1000,
			}
		)
	df = pd.DataFrame(rows)
	df.set_index(["date", "ticker", "exchange"], inplace=True)
	return df


def _make_dividend_df(date_str="2020-01-02", exchange="US", tickers=None, exchanges=None):
	if tickers is None:
		tickers = ["AAA", "BBB"]
	if exchanges is None:
		exchanges = [exchange] * len(tickers)
	rows = []
	for ticker, ex_code in zip(tickers, exchanges):
		rows.append(
			{
				"date": pd.Timestamp(date_str),
				"ticker": ticker,
				"exchange": ex_code,
				"currency": "USD",
				"declarationDate": "2020-01-01",
				"paymentDate": "2020-01-10",
				"period": "2020-01",
				"recordDate": "2020-01-05",
				"unadjustedValue": 0.5,
				"value": 0.5,
			}
		)
	df = pd.DataFrame(rows)
	df.set_index(["date", "ticker", "exchange"], inplace=True)
	return df


def _make_split_df(date_str="2020-01-02", exchange="US", tickers=None, exchanges=None):
	if tickers is None:
		tickers = ["AAA", "BBB"]
	if exchanges is None:
		exchanges = [exchange] * len(tickers)
	rows = []
	for ticker, ex_code in zip(tickers, exchanges):
		rows.append(
			{
				"date": pd.Timestamp(date_str),
				"ticker": ticker,
				"exchange": ex_code,
				"split": "2/1",
			}
		)
	df = pd.DataFrame(rows)
	df.set_index(["date", "ticker", "exchange"], inplace=True)
	return df


def _make_forex_df(date_str="2020-01-02", tickers=None):
	if tickers is None:
		tickers = ["USDEUR", "USDGBP"]
	rows = []
	for ticker in tickers:
		rows.append(
			{
				"date": pd.Timestamp(date_str),
				"ticker": ticker,
				"adjusted_close": 1.0,
				"close": 1.1,
				"high": 1.2,
				"low": 0.9,
				"open": 1.0,
				"volume": 1000,
			}
		)
	df = pd.DataFrame(rows)
	df.set_index(["date", "ticker"], inplace=True)
	return df


class TestDump(unittest.TestCase):
	def setUp(self):
		self.dump = Dump(testing=True)
		try:
			self.dump.delete()
		except Exception:
			pass

	def tearDown(self):
		try:
			self.dump.delete()
		except Exception:
			pass

	def test_write_read_exists_delete(self):
		df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
		self.dump.write({"test": df})
		self.assertTrue(self.dump.exists("test"))

		loaded = self.dump.read(["test"])["test"]
		pd.testing.assert_frame_equal(df, loaded)

		self.dump.delete()
		self.assertFalse(self.dump.exists("test"))


class TestStatic(unittest.TestCase):
	def setUp(self):
		self.static = Static()

	def test_get_currency(self):
		data = self.static.get_currency()
		self.assertFalse(data.empty)
		self.assertEqual(["ticker", "name", "country_code_list"], data.columns.tolist())

	def test_get_domicile(self):
		data = self.static.get_domicile()
		self.assertFalse(data.empty)
		self.assertEqual(["country_code", "country_name", "currency_ticker"], data.columns.tolist())

	def test_get_exchange(self):
		data = self.static.get_exchange()
		self.assertFalse(data.empty)
		self.assertEqual(["mic", "country_code", "exchange_name", "eod_code"], data.columns.tolist())


class TestMetaData(unittest.TestCase):
	def setUp(self):
		self.meta = MetaData()

	def test_get_etfs_filtered(self):
		data = self.meta.get_etfs()
		self.assertFalse(data.empty)
		self.assertIn("isin", data.columns)

	def test_get_indices_with_mock(self):
		mock_df = pd.DataFrame(
			[
				{"Name": "Index A", "Code": "AAA", "Currency": "USD"},
				{"Name": "Index B", "Code": "BBB", "Currency": "EUR"},
			]
		)
		with patch("src.asset_base.financial_data.Exchanges.get_indices", return_value=mock_df):
			data = self.meta.get_indices()
		self.assertEqual(["index_name", "ticker", "currency_code"], data.columns.tolist())
		self.assertEqual(["AAA", "BBB"], data["ticker"].tolist())


class TestHistory(unittest.TestCase):
	def setUp(self):
		self.history = History()
		ex_us = DummyExchange("US", "XNAS")
		ex_jse = DummyExchange("JSE", "XJSE")
		self.assets = [
			DummyAsset("AAA", ex_us, "ISINAAA", last_eod=datetime.date(2020, 1, 1),
					   last_dividend=datetime.date(2020, 1, 1), last_split=datetime.date(2020, 1, 1)),
			DummyAsset("BBB", ex_jse, "ISINBBB", last_eod=datetime.date(2020, 1, 1),
					   last_dividend=datetime.date(2020, 1, 1), last_split=datetime.date(2020, 1, 1)),
		]
		self.forex = [DummyForex("USDEUR", last_eod=datetime.date(2020, 1, 1))]
		self.indices = [DummyIndex("GSPC", last_eod=datetime.date(2020, 1, 1))]

	def test_get_eod_with_mock(self):
		mock_df = _make_eod_df(tickers=["AAA", "BBB"], exchanges=["US", "JSE"])
		with patch("src.asset_base.financial_data.MultiHistorical.get_eod", return_value=mock_df):
			data = self.history.get_eod(self.assets)
		self.assertIn("isin", data.columns)
		self.assertIn("date_stamp", data.columns)
		self.assertTrue(pd.api.types.is_datetime64_any_dtype(data["date_stamp"]))

	def test_get_dividends_with_mock(self):
		mock_df = _make_dividend_df(tickers=["AAA", "BBB"], exchanges=["US", "JSE"])
		with patch("src.asset_base.financial_data.MultiHistorical.get_dividends", return_value=mock_df):
			data = self.history.get_dividends(self.assets)
		expected_columns = [
			"date_stamp",
			"currency",
			"declaration_date",
			"payment_date",
			"period",
			"record_date",
			"unadjusted_value",
			"adjusted_value",
			"isin",
		]
		self.assertEqual(expected_columns, data.columns.tolist())
		self.assertTrue(pd.api.types.is_datetime64_any_dtype(data["date_stamp"]))

	def test_get_splits_with_mock(self):
		mock_df = _make_split_df(tickers=["AAA", "BBB"], exchanges=["US", "JSE"])
		with patch("src.asset_base.financial_data.MultiHistorical.get_splits", return_value=mock_df):
			data = self.history.get_splits(self.assets)
		expected_columns = ["date_stamp", "isin", "numerator", "denominator"]
		self.assertEqual(expected_columns, data.columns.tolist())
		self.assertTrue(pd.api.types.is_datetime64_any_dtype(data["date_stamp"]))

	def test_get_forex_with_mock(self):
		mock_df = _make_forex_df(tickers=["USDEUR"])
		with patch("src.asset_base.financial_data.MultiHistorical.get_forex", return_value=mock_df):
			data = self.history.get_forex(self.forex)
		expected_columns = [
			"date_stamp",
			"ticker",
			"adjusted_close",
			"close",
			"high",
			"low",
			"open",
			"volume",
		]
		self.assertEqual(expected_columns, data.columns.tolist())
		self.assertTrue(pd.api.types.is_datetime64_any_dtype(data["date_stamp"]))

	def test_get_indices_with_mock(self):
		mock_df = _make_forex_df(tickers=["GSPC"])
		with patch("src.asset_base.financial_data.MultiHistorical.get_index", return_value=mock_df):
			data = self.history.get_indices(self.indices)
		expected_columns = [
			"date_stamp",
			"ticker",
			"adjusted_close",
			"close",
			"high",
			"low",
			"open",
			"volume",
		]
		self.assertEqual(expected_columns, data.columns.tolist())
		self.assertTrue(pd.api.types.is_datetime64_any_dtype(data["date_stamp"]))


if __name__ == "__main__":
	unittest.main()
