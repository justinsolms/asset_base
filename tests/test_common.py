#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Unit tests for common module session management classes."""

import unittest
import tempfile
import os
import datetime
from unittest.mock import patch, MagicMock

import pandas as pd
from sqlalchemy import Column, Integer, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import database_exists

from src.asset_base.common import _Session, TestSession, SQLiteSession, Base, Common


class MockTable(Base):
    """Mock table class for testing database operations."""
    __tablename__ = "mock_table"

    id = Column(Integer, primary_key=True)
    name = Column(String(50))


class TestSessionBase(unittest.TestCase):
    """Base class for session testing with common utilities."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test databases
        self.temp_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.temp_dir, "test.db")
        self.test_db_url = f"sqlite:///{self.test_db_path}"

    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up temporary files
        if os.path.exists(self.test_db_path):
            try:
                os.unlink(self.test_db_path)
            except OSError:
                pass
        try:
            os.rmdir(self.temp_dir)
        except OSError:
            pass


class TestSessionClass(TestSessionBase):
    """Test the _Session abstract base class."""

    def test_init_creates_database_and_session(self):
        """Test that initialization creates database and session properly."""
        session_manager = _Session(self.test_db_url, testing=True)

        try:
            # Check that database file was created
            self.assertTrue(database_exists(self.test_db_url))

            # Check that session was created
            self.assertIsNotNone(session_manager.session)
            self.assertFalse(session_manager.is_closed)

            # Check that engine was created
            self.assertIsNotNone(session_manager.engine)

            # Test basic database operation
            mock_record = MockTable(name="test")
            session_manager.session.add(mock_record)
            session_manager.session.commit()

            # Verify record was saved
            count = session_manager.session.query(MockTable).count()
            self.assertEqual(count, 1)

        finally:
            session_manager.close()

    def test_init_with_echo_parameter(self):
        """Test initialization with echo parameter."""
        session_manager = _Session(self.test_db_url, testing=True, echo=True)

        try:
            self.assertTrue(session_manager.engine.echo)
        finally:
            session_manager.close()

    def test_close_is_idempotent(self):
        """Test that close() can be called multiple times safely."""
        session_manager = _Session(self.test_db_url, testing=True)

        # Close multiple times should not raise errors
        session_manager.close()
        self.assertTrue(session_manager.is_closed)

        session_manager.close()  # Should not raise
        self.assertTrue(session_manager.is_closed)

        # Session and engine should be None after closing
        self.assertIsNone(session_manager.session)
        self.assertIsNone(session_manager.engine)

    def test_new_session_creates_fresh_session(self):
        """Test that new_session() creates a fresh session."""
        session_manager = _Session(self.test_db_url, testing=True)

        try:
            original_session = session_manager.session

            # Create new session
            new_session = session_manager.new_session()

            # Should be different session objects
            self.assertIsNot(original_session, new_session)
            self.assertIs(new_session, session_manager.session)

        finally:
            session_manager.close()

    def test_new_session_after_close_raises_error(self):
        """Test that new_session() raises error after closing."""
        session_manager = _Session(self.test_db_url, testing=True)
        session_manager.close()

        with self.assertRaises(RuntimeError):
            session_manager.new_session()

    def test_context_manager_interface(self):
        """Test context manager functionality."""
        with _Session(self.test_db_url, testing=True) as session_manager:
            # Should be properly initialized
            self.assertIsNotNone(session_manager.session)
            self.assertFalse(session_manager.is_closed)

            # Test database operation
            mock_record = MockTable(name="context_test")
            session_manager.session.add(mock_record)
            session_manager.session.commit()

        # Should be closed after context exit
        self.assertTrue(session_manager.is_closed)

    def test_context_manager_with_exception(self):
        """Test context manager cleanup when exception occurs."""
        try:
            with _Session(self.test_db_url, testing=True) as session_manager:
                self.assertFalse(session_manager.is_closed)
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected

        # Should still be closed after exception
        self.assertTrue(session_manager.is_closed)

    def test_drop_database_removes_file(self):
        """Test that drop_database() removes the database file."""
        session_manager = _Session(self.test_db_url, testing=True)

        # Verify database exists
        self.assertTrue(database_exists(self.test_db_url))

        session_manager.close()
        session_manager.drop_database()

        # Database should no longer exist
        self.assertFalse(database_exists(self.test_db_url))

    def test_drop_nonexistent_database(self):
        """Test drop_database() with non-existent database."""
        # Use a non-existent database URL
        fake_url = f"sqlite:///{self.temp_dir}/nonexistent.db"
        session_manager = _Session(fake_url, testing=True)
        session_manager.close()

        # Should not raise error
        session_manager.drop_database()

    @patch('src.asset_base.common.create_engine')
    def test_init_failure_cleanup(self, mock_create_engine):
        """Test that initialization failure triggers proper cleanup."""
        mock_create_engine.side_effect = SQLAlchemyError("Mock engine creation failure")

        with self.assertRaises(SQLAlchemyError):
            _Session(self.test_db_url, testing=True)


class TestTestSession(TestSessionBase):
    """Test the TestSession class."""

    def test_init_creates_memory_database(self):
        """Test that TestSession creates in-memory database."""
        test_session = TestSession()

        try:
            # Should have session
            self.assertIsNotNone(test_session.session)

            # Should be in-memory SQLite
            self.assertTrue(test_session.db_url.startswith("sqlite://"))

            # Should be marked as testing
            self.assertTrue(test_session.testing)

            # Test basic operation
            mock_record = MockTable(name="memory_test")
            test_session.session.add(mock_record)
            test_session.session.commit()

            count = test_session.session.query(MockTable).count()
            self.assertEqual(count, 1)

        finally:
            test_session.close()

    def test_echo_parameter(self):
        """Test TestSession with echo parameter."""
        test_session = TestSession(echo=True)

        try:
            self.assertTrue(test_session.engine.echo)
        finally:
            test_session.close()

    def test_new_session_functionality(self):
        """Test new_session() with TestSession."""
        test_session = TestSession()

        try:
            # Add some data
            mock_record = MockTable(name="original")
            test_session.session.add(mock_record)
            test_session.session.commit()

            # Create new session
            new_session = test_session.new_session()

            # Data should still be there (same in-memory database)
            count = new_session.query(MockTable).count()
            self.assertEqual(count, 1)

        finally:
            test_session.close()


class TestSQLiteSession(TestSessionBase):
    """Test the SQLiteSession class."""

    def test_init_creates_file_database(self):
        """Test that SQLiteSession creates file-based database."""
        # Use a temporary location for testing
        with patch('src.asset_base.common.get_cache_path') as mock_get_path:
            mock_get_path.return_value = self.test_db_path

            sqlite_session = SQLiteSession(testing=False)

            try:
                # Should have session
                self.assertIsNotNone(sqlite_session.session)

                # Should be file-based SQLite
                self.assertEqual(sqlite_session.db_url, self.test_db_url)

                # Should not be marked as testing
                self.assertFalse(sqlite_session.testing)

                # Database file should exist
                self.assertTrue(os.path.exists(self.test_db_path))

                # Test basic operation
                mock_record = MockTable(name="file_test")
                sqlite_session.session.add(mock_record)
                sqlite_session.session.commit()

                count = sqlite_session.session.query(MockTable).count()
                self.assertEqual(count, 1)

            finally:
                sqlite_session.close()

    def test_testing_mode(self):
        """Test SQLiteSession in testing mode."""
        with patch('src.asset_base.common.get_cache_path') as mock_get_path:
            mock_get_path.return_value = self.test_db_path

            sqlite_session = SQLiteSession(testing=True)

            try:
                self.assertTrue(sqlite_session.testing)
            finally:
                sqlite_session.close()

    def test_echo_parameter(self):
        """Test SQLiteSession with echo parameter."""
        with patch('src.asset_base.common.get_cache_path') as mock_get_path:
            mock_get_path.return_value = self.test_db_path

            sqlite_session = SQLiteSession(echo=True)

            try:
                self.assertTrue(sqlite_session.engine.echo)
            finally:
                sqlite_session.close()


class TestSessionIntegration(TestSessionBase):
    """Integration tests for session classes."""

    def test_unittest_pattern_compatibility(self):
        """Test that sessions work with unittest setUp/tearDown pattern."""
        # Simulate unittest usage
        class MockTest:
            def setUp(self):
                self.test_session = TestSession()
                self.session = self.test_session.session

            def tearDown(self):
                del self.test_session

            def test_something(self):
                mock_record = MockTable(name="unittest_test")
                self.session.add(mock_record)
                self.session.commit()
                return self.session.query(MockTable).count()

        # Run mock test
        mock_test = MockTest()
        mock_test.setUp()

        try:
            count = mock_test.test_something()
            self.assertEqual(count, 1)
        finally:
            mock_test.tearDown()

    def test_session_refresh_pattern(self):
        """Test pattern for refreshing session after problematic operations."""
        test_session = TestSession()

        try:
            # Add some data
            mock_record = MockTable(name="before_refresh")
            test_session.session.add(mock_record)
            test_session.session.commit()

            # Simulate problematic operation that might leave cached state
            test_session.session.query(MockTable).delete()
            test_session.session.commit()

            # Refresh session to clear any cached relationships
            test_session.session = test_session.new_session()

            # Verify clean state
            count = test_session.session.query(MockTable).count()
            self.assertEqual(count, 0)

        finally:
            test_session.close()


class TestSessionErrorHandling(TestSessionBase):
    """Test error handling in session classes."""

    @patch('src.asset_base.common.logger')
    def test_del_method_warning(self, mock_logger):
        """Test that __del__ method logs warning when not properly closed."""
        session_manager = _Session(self.test_db_url, testing=True)

        # Don't close explicitly, let __del__ handle it
        session_manager.__del__()

        # Should have logged a warning
        mock_logger.warning.assert_called_once()
        self.assertIn("not properly closed", mock_logger.warning.call_args[0][0])

    def test_error_during_close(self):
        """Test error handling during close operations."""
        session_manager = _Session(self.test_db_url, testing=True)

        # Mock session.close to raise an error
        original_close = session_manager.session.close
        session_manager.session.close = MagicMock(side_effect=Exception("Mock close error"))

        # Close should still succeed and mark as closed
        session_manager.close()
        self.assertTrue(session_manager.is_closed)

        # Restore original method
        session_manager.session = None


# Create a concrete implementation of Common for testing
class ConcreteCommon(Common):
    """Concrete implementation of Common for testing."""
    __mapper_args__ = {
        "polymorphic_identity": "concrete_common",
    }

    def __str__(self):
        return f"ConcreteCommon({self.name})"

    def __repr__(self):
        return f"ConcreteCommon(name='{self.name}')"

    @property
    def key_code(self):
        return f"cc_{self.name}"

    @property
    def identity_code(self):
        return f"identity_{self.name}"

    @property
    def long_name(self):
        return f"Concrete Common: {self.name}"

    @classmethod
    def factory(cls, session, **kwargs):
        instance = cls(**kwargs)
        session.add(instance)
        return instance

    def to_dict(self):
        """Convert instance to dictionary for DataFrame export."""
        return {
            'name': self.name,
        }


class TestCommon(unittest.TestCase):
    """Test the abstract methods in Common class."""

    @classmethod
    def setUpClass(cls):
        """Set up test class fixtures."""
        cls.name = "Concrete common instance"

    def setUp(self):
        """Set up test case fixtures."""
        # Test with TestSession
        self.test_session = TestSession()
        self.session = self.test_session.session
        # Instantiate ConcreteCommon
        self.concrete_common = ConcreteCommon(name=self.name)

    def tearDown(self):
        """Tear down test case fixtures."""
        self.test_session.close()

    def test_class_name_abstract_method(self):
        """Test that class_name property returns the class name."""

        # Get the test case instance fixture
        instance = self.concrete_common

        # Test that class_name returns the correct class name
        self.assertEqual(instance.class_name, "ConcreteCommon")
        self.assertIsInstance(instance.class_name, str)

    def test_init_sets_name(self):
        """Test that initialization sets name correctly."""
        # Use the fixture
        self.assertEqual(self.concrete_common.name, self.name)

    def test_init_sets_creation_date(self):
        """Test that initialization sets date_create."""
        self.assertIsNotNone(self.concrete_common.date_create)
        # date_create could be datetime or date depending on implementation
        # Check that it's today
        if isinstance(self.concrete_common.date_create, datetime.datetime):
            self.assertEqual(self.concrete_common.date_create.date(), datetime.datetime.today().date())
        else:
            self.assertEqual(self.concrete_common.date_create, datetime.datetime.today().date())

    def test_date_mod_stamp_initially_none(self):
        """Test that date_mod_stamp is initially None."""
        self.assertIsNone(self.concrete_common.date_mod_stamp)

    def test_date_mod_stamp_can_be_set(self):
        """Test that date_mod_stamp can be set."""
        test_date = datetime.date(2025, 1, 1)
        self.concrete_common.date_mod_stamp = test_date
        self.assertEqual(self.concrete_common.date_mod_stamp, test_date)

    def test_str_method(self):
        """Test __str__ method returns correct format."""
        result = str(self.concrete_common)
        expected = f"ConcreteCommon({self.name})"
        self.assertEqual(result, expected)

    def test_repr_method(self):
        """Test __repr__ method returns correct format."""
        result = repr(self.concrete_common)
        expected = f"ConcreteCommon(name='{self.name}')"
        self.assertEqual(result, expected)

    def test_key_code_property(self):
        """Test key_code property returns expected value."""
        expected = f"cc_{self.name}"
        self.assertEqual(self.concrete_common.key_code, expected)

    def test_identity_code_property(self):
        """Test identity_code property returns expected value."""
        expected = f"identity_{self.name}"
        self.assertEqual(self.concrete_common.identity_code, expected)

    def test_factory_creates_instance(self):
        """Test factory method creates instances correctly."""
        instance = ConcreteCommon.factory(self.session, name="Factory Test")
        self.assertIsNotNone(instance)
        self.assertEqual(instance.name, "Factory Test")
        self.assertIsInstance(instance, ConcreteCommon)

    def test_database_persistence(self):
        """Test that instances can be persisted to database."""
        self.session.add(self.concrete_common)
        self.session.commit()

        # Query back from database
        retrieved = self.session.query(ConcreteCommon).filter_by(name=self.name).first()
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, self.name)

    def test_unique_name_constraint(self):
        """Test that name uniqueness is enforced per discriminator."""
        instance1 = ConcreteCommon(name="Duplicate")
        self.session.add(instance1)
        self.session.commit()

        # Try to add another with same name
        instance2 = ConcreteCommon(name="Duplicate")
        self.session.add(instance2)

        with self.assertRaises(SQLAlchemyError):
            self.session.commit()

    def test_key_code_id_table(self):
        """Test key_code_id_table returns correct DataFrame."""
        # Add multiple instances
        instance1 = ConcreteCommon(name="Entity1")
        instance2 = ConcreteCommon(name="Entity2")
        self.session.add(instance1)
        self.session.add(instance2)
        self.session.commit()

        # Get the table
        df = ConcreteCommon.key_code_id_table(self.session)

        # Verify structure
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertIn("id", df.columns)
        self.assertIn(ConcreteCommon.KEY_CODE_LABEL, df.columns)

        # Verify content
        self.assertIn("cc_Entity1", df[ConcreteCommon.KEY_CODE_LABEL].values)
        self.assertIn("cc_Entity2", df[ConcreteCommon.KEY_CODE_LABEL].values)

    def test_key_code_id_table_empty_database(self):
        """Test key_code_id_table with empty database."""
        df = ConcreteCommon.key_code_id_table(self.session)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)

    def test_from_data_frame(self):
        """Test from_data_frame creates instances from DataFrame."""
        # Create DataFrame
        data = pd.DataFrame({
            'name': ['Item1', 'Item2', 'Item3']
        })

        # Create instances from DataFrame
        ConcreteCommon.from_data_frame(self.session, data)
        self.session.commit()

        # Verify instances were created
        count = self.session.query(ConcreteCommon).count()
        self.assertEqual(count, 3)

        # Verify names
        items = self.session.query(ConcreteCommon).all()
        names = [item.name for item in items]
        self.assertIn('Item1', names)
        self.assertIn('Item2', names)
        self.assertIn('Item3', names)

    def test_from_data_frame_empty_dataframe(self):
        """Test from_data_frame handles empty DataFrame."""
        empty_df = pd.DataFrame()
        # Should not raise an error
        ConcreteCommon.from_data_frame(self.session, empty_df)
        self.session.commit()

        # No instances should be created
        count = self.session.query(ConcreteCommon).count()
        self.assertEqual(count, 0)

    def test_to_data_frame(self):
        """Test to_data_frame exports instances correctly."""
        # Create some instances
        instance1 = ConcreteCommon(name="Export1")
        instance2 = ConcreteCommon(name="Export2")
        self.session.add(instance1)
        self.session.add(instance2)
        self.session.commit()

        # Export to DataFrame
        df = ConcreteCommon.to_data_frame(self.session)

        # Verify structure
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertIn('name', df.columns)

        # Verify content
        self.assertIn('Export1', df['name'].values)
        self.assertIn('Export2', df['name'].values)

    def test_to_data_frame_empty_database(self):
        """Test to_data_frame with empty database."""
        df = ConcreteCommon.to_data_frame(self.session)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)

    def test_discriminator_field(self):
        """Test that discriminator field is set correctly."""
        self.session.add(self.concrete_common)
        self.session.commit()

        # Verify discriminator
        self.assertEqual(self.concrete_common._discriminator, "concrete_common")

    def test_primary_key_autoincrement(self):
        """Test that primary key auto-increments."""
        instance1 = ConcreteCommon(name="First")
        instance2 = ConcreteCommon(name="Second")

        self.session.add(instance1)
        self.session.commit()
        id1 = instance1._id

        self.session.add(instance2)
        self.session.commit()
        id2 = instance2._id

        self.assertIsNotNone(id1)
        self.assertIsNotNone(id2)
        self.assertGreater(id2, id1)

    def test_name_cannot_be_null(self):
        """Test that name field is required."""
        # This should fail at the Python level, not database level
        with self.assertRaises(TypeError):
            ConcreteCommon()

    def test_multiple_instances_with_different_names(self):
        """Test multiple instances with different names coexist."""
        instances = [ConcreteCommon(name=f"Instance{i}") for i in range(5)]
        for instance in instances:
            self.session.add(instance)
        self.session.commit()

        count = self.session.query(ConcreteCommon).count()
        self.assertEqual(count, 5)

    def test_instance_update(self):
        """Test updating an instance's attributes."""
        instance = ConcreteCommon(name="Original")
        self.session.add(instance)
        self.session.commit()

        # Update date_mod_stamp
        new_date = datetime.date(2025, 6, 15)
        instance.date_mod_stamp = new_date
        self.session.commit()

        # Retrieve and verify
        retrieved = self.session.query(ConcreteCommon).filter_by(name="Original").first()
        self.assertEqual(retrieved.date_mod_stamp, new_date)

    def test_session_query_filter(self):
        """Test querying instances with filters."""
        instance1 = ConcreteCommon(name="Alpha")
        instance2 = ConcreteCommon(name="Beta")
        instance3 = ConcreteCommon(name="Gamma")

        self.session.add_all([instance1, instance2, instance3])
        self.session.commit()

        # Query specific instance
        result = self.session.query(ConcreteCommon).filter_by(name="Beta").first()
        self.assertIsNotNone(result)
        self.assertEqual(result.name, "Beta")

    def test_key_code_label_class_attribute(self):
        """Test that KEY_CODE_LABEL class attribute exists."""
        self.assertTrue(hasattr(self.concrete_common, 'KEY_CODE_LABEL'))
        self.assertIsInstance(self.concrete_common.KEY_CODE_LABEL, str)




if __name__ == "__main__":
    # Create test suite
    suite = unittest.TestSuite()

    # Add all test classes
    test_classes = [
        TestSessionClass,
        TestTestSession,
        TestSQLiteSession,
        TestSessionIntegration,
        TestSessionErrorHandling,
        TestCommon
    ]

    loader = unittest.TestLoader()
    for test_class in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(test_class))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with appropriate code
    exit(0 if result.wasSuccessful() else 1)