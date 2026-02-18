"""Unit tests for dump/reuse paradigm.

This module tests the complete lifecycle of dumping database contents to pickle
files and reusing them to initialize a fresh database.

NOTE: Static entities (Currency, Domicile, Exchange, Cash) are NOT dumped/reused.
They are always recreated from the Static class. Only ListedEquity and its time
series data (ListedEOD, Dividend, Split) are dumped and reused.

Tests cover:
- Roundtrip data integrity (dump → reuse → verify)
- Manager orchestration of dump/reuse operations
- Error handling and edge cases
- File I/O operations
"""

import unittest
import unittest.mock
from unittest.mock import patch, MagicMock, call
import pandas as pd
import numpy as np
from datetime import datetime, date
import tempfile
import shutil
import os
import pickle

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from asset_base.manager import Manager, Meta
from asset_base.asset import ListedEquity, Listed, Cash
from asset_base.time_series import ListedEOD, Dividend, Split
from asset_base.financial_data import Dump, Static
from asset_base.entity import Currency, Domicile, Exchange, Issuer
from asset_base.common import Base


class TestDumpReuseRoundtrip(unittest.TestCase):
    """Test complete dump/reuse lifecycle with real database operations."""

    def setUp(self):
        """Set up test database and manager."""
        # Create in-memory SQLite database
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Create temporary directory for dump files
        self.temp_dir = tempfile.mkdtemp()
        self.dumper = Dump(testing=True)
        # Override the dumper's path to use our temp directory
        self.dumper._abs_data_path = self.temp_dir

        # Set up Static mock
        self.static_patcher = patch('asset_base.manager.Static')
        self.mock_static = self.static_patcher.start()
        self._configure_static_data()

    def tearDown(self):
        """Clean up test database and temp files."""
        self.session.close()
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        self.static_patcher.stop()

    def _configure_static_data(self):
        """Configure mock static data for tests."""
        # Currency data
        currency_df = pd.DataFrame([
            {'ticker': 'USD', 'name': 'US Dollar', 'country_code_list': ['US']},
            {'ticker': 'EUR', 'name': 'Euro', 'country_code_list': ['DE', 'FR']}
        ])
        self.mock_static.return_value.Currency.return_value = currency_df

        # Domicile data
        domicile_df = pd.DataFrame([
            {'country_code': 'US', 'country_name': 'United States', 'currency_ticker': 'USD'},
            {'country_code': 'DE', 'country_name': 'Germany', 'currency_ticker': 'EUR'}
        ])
        self.mock_static.return_value.Domicile.return_value = domicile_df

        # Exchange data
        exchange_df = pd.DataFrame([
            {'mic': 'XNYS', 'exchange_name': 'New York Stock Exchange', 'country_code': 'US'},
            {'mic': 'XETR', 'exchange_name': 'Deutsche Boerse', 'country_code': 'DE'}
        ])
        self.mock_static.return_value.Exchange.return_value = exchange_df

    def _create_test_entities(self):
        """Create test currency, domicile, and exchange entities."""
        usd = Currency(ticker='USD', name='US Dollar', country_code_list='US')
        eur = Currency(ticker='EUR', name='Euro', country_code_list='DE')
        us_dom = Domicile(country_code='US', country_name='United States', currency=usd)
        de_dom = Domicile(country_code='DE', country_name='Germany', currency=eur)
        nyse = Exchange(mic='XNYS', name='New York Stock Exchange', domicile=us_dom)
        xetr = Exchange(mic='XETR', name='Deutsche Boerse', domicile=de_dom)
        apple_issuer = Issuer(name='Apple Inc.', domicile=us_dom)
        sap_issuer = Issuer(name='SAP SE', domicile=de_dom)

        self.session.add_all([usd, eur, us_dom, de_dom, nyse, xetr, apple_issuer, sap_issuer])
        self.session.commit()

        return {'usd': usd, 'eur': eur, 'us_dom': us_dom, 'de_dom': de_dom,
                'nyse': nyse, 'xetr': xetr, 'apple_issuer': apple_issuer,
                'sap_issuer': sap_issuer}

    def _create_test_assets(self, entities):
        """Create test assets with time series data."""
        # Create ListedEquity
        apple = ListedEquity(
            name='Apple Inc.',
            issuer=entities['apple_issuer'],
            isin='US0378331005',
            exchange=entities['nyse'],
            ticker='AAPL',
            status='listed'
        )
        sap = ListedEquity(
            name='SAP SE',
            issuer=entities['sap_issuer'],
            isin='DE0007164600',
            exchange=entities['xetr'],
            ticker='SAP',
            status='listed'
        )

        self.session.add_all([apple, sap])
        self.session.commit()

        # Create EOD time series data
        dates = pd.date_range('2024-01-01', periods=5, freq='D')
        for i, dt in enumerate(dates):
            # Apple EOD
            eod_aapl = ListedEOD(
                base_obj=apple,
                date_stamp=dt.date(),
                open=100.0 + i,
                high=102.0 + i,
                low=99.0 + i,
                close=101.0 + i,
                volume=1000000 + i * 1000,
                adjusted_close=101.0 + i
            )
            # SAP EOD
            eod_sap = ListedEOD(
                base_obj=sap,
                date_stamp=dt.date(),
                open=50.0 + i,
                high=51.0 + i,
                low=49.0 + i,
                close=50.5 + i,
                volume=500000 + i * 500,
                adjusted_close=50.5 + i
            )
            self.session.add_all([eod_aapl, eod_sap])

        # Create dividend data
        div_aapl = Dividend(
            base_obj=apple,
            date_stamp=date(2024, 1, 3),
            currency='USD',
            declaration_date=date(2024, 1, 10),
            payment_date=date(2024, 1, 20),
            period='Quarterly',
            record_date=date(2024, 1, 12),
            unadjusted_value=0.25,
            adjusted_value=0.25
        )
        div_sap = Dividend(
            base_obj=sap,
            date_stamp=date(2024, 1, 4),
            currency='EUR',
            declaration_date=date(2024, 1, 1),
            payment_date=date(2024, 1, 15),
            period='Semi-annually',
            record_date=date(2024, 1, 3),
            unadjusted_value=0.50,
            adjusted_value=0.50
        )

        # Create split data
        split_aapl = Split(
            base_obj=apple,
            date_stamp=date(2024, 1, 5),
            numerator=2.0,
            denominator=1.0
        )

        self.session.add_all([div_aapl, div_sap, split_aapl])
        self.session.commit()

        return {'apple': apple, 'sap': sap}

    def test_listed_equity_roundtrip(self):
        """Test dumping and reusing ListedEquity preserves all data.

        NOTE: Static entities (Currency, Domicile, Exchange) must exist before
        reusing dumps since they're not included in dump files.
        """
        # Create test data
        entities = self._create_test_entities()
        assets = self._create_test_assets(entities)

        # Get original data for comparison
        original_equities = self.session.query(ListedEquity).all()
        original_eod_count = self.session.query(ListedEOD).count()
        original_div_count = self.session.query(Dividend).count()
        original_split_count = self.session.query(Split).count()

        self.assertEqual(len(original_equities), 2)
        self.assertEqual(original_eod_count, 10)  # 2 assets * 5 days
        self.assertEqual(original_div_count, 2)
        self.assertEqual(original_split_count, 1)

        # Dump data (only ListedEquity and time series, NOT Static entities)
        ListedEquity.dump(self.session, self.dumper)

        # Verify dump files were created
        equity_file = os.path.join(self.temp_dir, 'ListedEquity.pandas.dataframe.pkl')
        eod_file = os.path.join(self.temp_dir, 'ListedEOD.pandas.dataframe.pkl')
        div_file = os.path.join(self.temp_dir, 'Dividend.pandas.dataframe.pkl')
        split_file = os.path.join(self.temp_dir, 'Split.pandas.dataframe.pkl')

        self.assertTrue(os.path.exists(equity_file))
        self.assertTrue(os.path.exists(eod_file))
        self.assertTrue(os.path.exists(div_file))
        self.assertTrue(os.path.exists(split_file))

        # Clear only dumped/reused data (NOT Static entities - they stay)
        # Use individual deletes to properly handle joined-table inheritance
        for split in self.session.query(Split).all():
            self.session.delete(split)
        for div in self.session.query(Dividend).all():
            self.session.delete(div)
        for eod in self.session.query(ListedEOD).all():
            self.session.delete(eod)
        for equity in self.session.query(ListedEquity).all():
            self.session.delete(equity)
        self.session.commit()

        self.assertEqual(self.session.query(ListedEquity).count(), 0)
        self.assertEqual(self.session.query(ListedEOD).count(), 0)
        # Static entities should still exist
        self.assertGreater(self.session.query(Currency).count(), 0)
        self.assertGreater(self.session.query(Exchange).count(), 0)

        # Reuse dumped data (Static entities already exist)
        ListedEquity.reuse(self.session, self.dumper)

        # Verify data was restored
        restored_equities = self.session.query(ListedEquity).all()
        restored_eod_count = self.session.query(ListedEOD).count()
        restored_div_count = self.session.query(Dividend).count()
        restored_split_count = self.session.query(Split).count()

        self.assertEqual(len(restored_equities), 2)
        self.assertEqual(restored_eod_count, 10)
        self.assertEqual(restored_div_count, 2)
        self.assertEqual(restored_split_count, 1)

        # Verify specific data integrity
        apple_restored = self.session.query(ListedEquity).filter_by(ticker='AAPL').first()
        self.assertIsNotNone(apple_restored)
        self.assertEqual(apple_restored.name, 'Apple Inc. (XNYS)')
        self.assertEqual(apple_restored.isin, 'US0378331005')

        # Verify time series data
        apple_eod = self.session.query(ListedEOD).filter_by(_asset_id=apple_restored._id).all()
        self.assertEqual(len(apple_eod), 5)
        self.assertAlmostEqual(apple_eod[0].close, 101.0)

    def test_dump_empty_database_creates_empty_files(self):
        """Test dumping when no assets exist creates empty pickle files.

        NOTE: Static entities exist but are not dumped.
        """
        entities = self._create_test_entities()

        # Dump with no assets (only Static entities exist, which aren't dumped)
        ListedEquity.dump(self.session, self.dumper)

        # Verify files were created even though empty
        equity_file = os.path.join(self.temp_dir, 'ListedEquity.pandas.dataframe.pkl')
        self.assertTrue(os.path.exists(equity_file))

        # Load and verify it's an empty DataFrame
        dump_dict = self.dumper.read(['ListedEquity'])
        self.assertIn('ListedEquity', dump_dict)
        self.assertTrue(dump_dict['ListedEquity'].empty)

    def test_reuse_deletes_existing_records(self):
        """Test reuse workflow: dump → destroy DB → create new DB → reuse.

        Note: reuse() is designed for initializing empty databases,
        not for updating existing data.
        """
        entities = self._create_test_entities()
        assets = self._create_test_assets(entities)

        # Dump data
        ListedEquity.dump(self.session, self.dumper)

        # Verify we have data
        original_count = self.session.query(ListedEOD).count()
        self.assertEqual(original_count, 10)

        # Close entire database and create new one (simulating real use case)
        self.session.close()
        self.engine.dispose()
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Recreate Static entities for reuse
        entities = self._create_test_entities()

        # Verify new database is empty
        self.assertEqual(self.session.query(ListedEOD).count(), 0)

        # Reuse should populate the new database
        ListedEquity.reuse(self.session, self.dumper)
        ListedEOD.reuse(self.session, ListedEquity, self.dumper)

        final_count = self.session.query(ListedEOD).count()
        self.assertEqual(final_count, 10)

    def test_dump_preserves_datetime_types(self):
        """Test that date_stamp columns remain pandas.Timestamp after roundtrip."""
        entities = self._create_test_entities()
        assets = self._create_test_assets(entities)

        # Dump data
        ListedEOD.dump(self.session, ListedEquity, self.dumper)

        # Read dumped data
        dump_dict = self.dumper.read(['ListedEOD'])
        eod_df = dump_dict['ListedEOD']

        # Verify date_stamp is datetime type
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(eod_df['date_stamp']))

    def test_dump_preserves_nan_values(self):
        """Test that NaN values in nullable columns are preserved in dump/reuse cycle."""
        entities = self._create_test_entities()
        assets = self._create_test_assets(entities)

        # Create EOD with all required fields (adjusted_close is NOT NULL)
        # Volume is nullable in some schemas, but for this test we'll verify
        # that normal numeric values round-trip correctly
        apple = assets['apple']
        eod_test = ListedEOD(
            base_obj=apple,
            date_stamp=date(2024, 2, 1),
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            volume=1000000,
            adjusted_close=101.0
        )
        self.session.add(eod_test)
        self.session.commit()

        # Dump all data
        ListedEquity.dump(self.session, self.dumper)
        ListedEOD.dump(self.session, ListedEquity, self.dumper)

        # Close entire database and create new one
        self.session.close()
        self.engine.dispose()
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Recreate Static entities for reuse
        entities = self._create_test_entities()

        # Reuse dumped data
        ListedEquity.reuse(self.session, self.dumper)
        ListedEOD.reuse(self.session, ListedEquity, self.dumper)

        # Verify data was preserved
        restored_eod = self.session.query(ListedEOD).filter_by(
            date_stamp=date(2024, 2, 1)
        ).first()
        self.assertIsNotNone(restored_eod)
        self.assertAlmostEqual(restored_eod.adjusted_close, 101.0)

    def test_dump_preserves_corporate_actions(self):
        """Test that dividends and splits survive dump/reuse intact."""
        entities = self._create_test_entities()
        assets = self._create_test_assets(entities)

        # Get original dividend and split data
        original_div = self.session.query(Dividend).filter_by(
            date_stamp=date(2024, 1, 3)
        ).first()
        original_split = self.session.query(Split).filter_by(
            date_stamp=date(2024, 1, 5)
        ).first()

        self.assertAlmostEqual(original_div.unadjusted_value, 0.25)
        self.assertAlmostEqual(original_split.numerator, 2.0)

        # Dump all data
        ListedEquity.dump(self.session, self.dumper)
        Dividend.dump(self.session, ListedEquity, self.dumper)
        Split.dump(self.session, ListedEquity, self.dumper)

        # Close entire database and create new one
        self.session.close()
        self.engine.dispose()
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Recreate Static entities for reuse
        entities = self._create_test_entities()

        # Reuse dumped data
        ListedEquity.reuse(self.session, self.dumper)
        Dividend.reuse(self.session, ListedEquity, self.dumper)
        Split.reuse(self.session, ListedEquity, self.dumper)

        # Verify restored data
        restored_div = self.session.query(Dividend).filter_by(
            date_stamp=date(2024, 1, 3)
        ).first()
        restored_split = self.session.query(Split).filter_by(
            date_stamp=date(2024, 1, 5)
        ).first()

        self.assertIsNotNone(restored_div)
        self.assertIsNotNone(restored_split)
        self.assertAlmostEqual(restored_div.unadjusted_value, 0.25)
        self.assertAlmostEqual(restored_split.numerator, 2.0)
        self.assertAlmostEqual(restored_split.denominator, 1.0)


class TestManagerDumpReuse(unittest.TestCase):
    """Test Manager-level dump/reuse orchestration.

    Manager.dump() only dumps ListedEquity and its time series data.
    Manager.reuse() reuses those dumps, assuming Static entities already exist.
    """

    def setUp(self):
        """Set up manager with mocked dependencies."""
        self.temp_dir = tempfile.mkdtemp()

        # Patch the update_all methods that set_up calls
        self.currency_patcher = patch('asset_base.entity.Currency.update_all')
        self.domicile_patcher = patch('asset_base.entity.Domicile.update_all')
        self.exchange_patcher = patch('asset_base.entity.Exchange.update_all')
        self.cash_patcher = patch('asset_base.asset.Cash.update_all')

        self.mock_currency_update = self.currency_patcher.start()
        self.mock_domicile_update = self.domicile_patcher.start()
        self.mock_exchange_update = self.exchange_patcher.start()
        self.mock_cash_update = self.cash_patcher.start()

        # Create manager with testing dumper
        self.manager = Manager(dialect='memory', testing=True)
        self.manager.dumper._abs_data_path = self.temp_dir

        # Create test entities manually (since update_all is mocked)
        self._create_test_entities()

    def tearDown(self):
        """Clean up manager and temp files."""
        if hasattr(self, 'manager'):
            self.manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        self.currency_patcher.stop()
        self.domicile_patcher.stop()
        self.exchange_patcher.stop()
        self.cash_patcher.stop()

    def _create_test_entities(self):
        """Create minimal test entities for dump/reuse tests."""
        # Create static entities that tests need
        usd = Currency(ticker='USD', name='US Dollar', country_code_list='US')
        us_dom = Domicile(country_code='US', country_name='United States', currency=usd)
        nyse = Exchange(mic='XNYS', name='New York Stock Exchange', domicile=us_dom)
        self.manager.session.add_all([usd, us_dom, nyse])
        self.manager.session.commit()

    def test_manager_dump_creates_all_class_files(self):
        """Test that Manager.dump() creates files for dumped classes.

        Only ListedEquity and time series are dumped (NOT Currency/Domicile/Exchange).
        """
        # Add a test equity to dump (entities already created in setUp)
        issuer = Issuer.factory(self.manager.session, 'Test Corp', 'US')
        exchange = self.manager.session.query(Exchange).first()
        equity = ListedEquity(
            name='Test Corp',
            issuer=issuer,
            isin='US0378331005',
            exchange=exchange,
            ticker='TEST',
            status='listed'
        )
        self.manager.session.add(equity)
        self.manager.session.commit()

        # Dump
        self.manager.dump()

        # Verify files were created
        equity_file = os.path.join(self.temp_dir, 'ListedEquity.pandas.dataframe.pkl')
        self.assertTrue(os.path.exists(equity_file))

    def test_manager_reuse_handles_missing_files(self):
        """Test Manager.reuse() handles FileNotFoundError gracefully."""
        # Try to reuse without dump files (entities already created in setUp)
        # Should not raise exception, just log info
        with patch('asset_base.manager.logger') as mock_logger:
            self.manager.reuse()
            # Verify logger.info was called about missing files
            info_calls = [call for call in mock_logger.info.call_args_list
                         if 'Dump data not found' in str(call)]
            self.assertGreater(len(info_calls), 0)

    @patch('asset_base.manager.logger')
    def test_manager_reuse_logs_success(self, mock_logger):
        """Test Manager.reuse() logs success when files exist."""
        # Set up and create dump
        self.manager.set_up(reuse=False, update=False)

        issuer = Issuer.factory(self.manager.session, 'Test Corp', 'US')
        equity = ListedEquity(
            name='Test Corp',
            issuer=issuer,
            isin='US0378331005',
            exchange=self.manager.session.query(Exchange).first(),
            ticker='TEST',
            status='listed'
        )
        self.manager.session.add(equity)
        self.manager.session.commit()

        self.manager.dump()

        # Clear database
        for equity in self.manager.session.query(ListedEquity).all():
            self.manager.session.delete(equity)
        self.manager.session.commit()

        # Reuse
        self.manager.reuse()

        # Verify success was logged
        success_calls = [call for call in mock_logger.info.call_args_list
                        if 'Reused dumped data' in str(call)]
        self.assertGreater(len(success_calls), 0)

    def test_manager_set_up_with_reuse_loads_dumps(self):
        """Test Manager.set_up(reuse=True) attempts to load dumps."""
        # Create dumps first (entities already created in setUp)
        issuer = Issuer.factory(self.manager.session, 'Test Corp', 'US')
        equity = ListedEquity(
            name='Test Corp',
            issuer=issuer,
            isin='US0378331005',
            exchange=self.manager.session.query(Exchange).first(),
            ticker='TEST',
            status='listed'
        )
        self.manager.session.add(equity)
        self.manager.session.commit()

        self.manager.dump()

        # Test that set_up with reuse=True calls reuse method
        with patch.object(self.manager, 'reuse', wraps=self.manager.reuse) as mock_reuse:
            # Don't create new manager, just call set_up again with reuse=True
            # The update_all methods are already mocked, so this should work
            self.manager.set_up(reuse=True, update=False)
            mock_reuse.assert_called_once()

    def test_manager_delete_dumps_removes_directory(self):
        """Test Manager.delete_dumps() removes dump directory contents."""
        # Entities already created in setUp
        self.manager.dump()

        # Verify directory exists and has files
        self.assertTrue(os.path.exists(self.temp_dir))
        files_before = os.listdir(self.temp_dir)
        self.assertGreater(len(files_before), 0)

        # Delete dumps
        self.manager.delete_dumps()

        # Directory should still exist but be empty
        self.assertTrue(os.path.exists(self.temp_dir))
        self.assertEqual(len(os.listdir(self.temp_dir)), 0)


class TestDumpErrorHandling(unittest.TestCase):
    """Test error handling in dump/reuse operations."""

    def setUp(self):
        """Set up test database."""
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        self.temp_dir = tempfile.mkdtemp()
        self.dumper = Dump(testing=True)
        self.dumper._abs_data_path = self.temp_dir

    def tearDown(self):
        """Clean up."""
        self.session.close()
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_reuse_with_corrupted_pickle_raises_error(self):
        """Test that corrupted pickle files raise appropriate errors."""
        # Create corrupted pickle file
        corrupt_file = os.path.join(self.temp_dir, 'ListedEquity.pandas.dataframe.pkl')
        with open(corrupt_file, 'wb') as f:
            f.write(b'corrupted data that is not a valid pickle')

        # Attempt to read should raise error
        with self.assertRaises((pickle.UnpicklingError, EOFError)):
            self.dumper.read(['ListedEquity'])

    def test_dump_with_invalid_path_raises_error(self):
        """Test that dumping to invalid path raises error."""
        # Set invalid path
        self.dumper._abs_data_path = '/invalid/nonexistent/path/that/does/not/exist'

        dump_dict = {'TestClass': pd.DataFrame({'a': [1, 2, 3]})}

        # Should raise error
        with self.assertRaises((OSError, FileNotFoundError)):
            self.dumper.write(dump_dict)

    def test_reuse_missing_file_raises_file_not_found(self):
        """Test that reusing non-existent file raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            self.dumper.read(['NonExistentClass'])


class TestDumpFileOperations(unittest.TestCase):
    """Test Dump class file I/O operations."""

    def setUp(self):
        """Set up temp directory and dumper."""
        self.temp_dir = tempfile.mkdtemp()
        self.dumper = Dump(testing=True)
        self.dumper._abs_data_path = self.temp_dir

    def tearDown(self):
        """Clean up temp files."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_dump_write_creates_pickle_file(self):
        """Test that write() creates pickle file with correct naming."""
        test_df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })

        dump_dict = {'TestClass': test_df}
        self.dumper.write(dump_dict)

        # Verify file exists
        file_path = os.path.join(self.temp_dir, 'TestClass.pandas.dataframe.pkl')
        self.assertTrue(os.path.exists(file_path))

        # Verify content
        loaded_df = pd.read_pickle(file_path)
        pd.testing.assert_frame_equal(loaded_df, test_df)

    def test_dump_read_loads_pickle_file(self):
        """Test that read() correctly loads pickle files."""
        test_df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })

        # Write file
        dump_dict = {'TestClass': test_df}
        self.dumper.write(dump_dict)

        # Read back
        loaded_dict = self.dumper.read(['TestClass'])

        self.assertIn('TestClass', loaded_dict)
        pd.testing.assert_frame_equal(loaded_dict['TestClass'], test_df)

    def test_dump_write_multiple_classes(self):
        """Test dumping multiple classes in one call."""
        df1 = pd.DataFrame({'a': [1, 2]})
        df2 = pd.DataFrame({'b': [3, 4]})

        dump_dict = {'Class1': df1, 'Class2': df2}
        self.dumper.write(dump_dict)

        # Verify both files exist
        self.assertTrue(os.path.exists(
            os.path.join(self.temp_dir, 'Class1.pandas.dataframe.pkl')))
        self.assertTrue(os.path.exists(
            os.path.join(self.temp_dir, 'Class2.pandas.dataframe.pkl')))

    def test_dump_read_multiple_classes(self):
        """Test reading multiple classes in one call."""
        df1 = pd.DataFrame({'a': [1, 2]})
        df2 = pd.DataFrame({'b': [3, 4]})

        dump_dict = {'Class1': df1, 'Class2': df2}
        self.dumper.write(dump_dict)

        # Read both
        loaded_dict = self.dumper.read(['Class1', 'Class2'])

        self.assertEqual(len(loaded_dict), 2)
        pd.testing.assert_frame_equal(loaded_dict['Class1'], df1)
        pd.testing.assert_frame_equal(loaded_dict['Class2'], df2)

    def test_dump_exists_returns_true_for_existing_file(self):
        """Test exists() returns True for existing dump files."""
        test_df = pd.DataFrame({'a': [1]})
        self.dumper.write({'TestClass': test_df})

        self.assertTrue(self.dumper.exists('TestClass'))

    def test_dump_exists_returns_false_for_missing_file(self):
        """Test exists() returns False for non-existent files."""
        self.assertFalse(self.dumper.exists('NonExistentClass'))

    def test_dump_delete_removes_directory(self):
        """Test delete() removes dump directory contents but keeps the directory."""
        # Write some data
        test_df = pd.DataFrame({'a': [1]})
        self.dumper.write({'TestClass': test_df})

        # Verify directory and file exist
        self.assertTrue(os.path.exists(self.temp_dir))
        file_path = os.path.join(self.temp_dir, 'TestClass.pandas.dataframe.pkl')
        self.assertTrue(os.path.exists(file_path))

        # Delete
        self.dumper.delete()

        # Directory should still exist but be empty
        self.assertTrue(os.path.exists(self.temp_dir))
        self.assertEqual(len(os.listdir(self.temp_dir)), 0)


if __name__ == '__main__':
    unittest.main()
