#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Declare common object infrastructure
"""
from abc import ABC

import sys
import datetime
import logging

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy import Integer, String, Date, Column, UniqueConstraint
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy_utils import drop_database, database_exists, create_database  # type: ignore

from asset_base import get_cache_path


logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Create the declarative base
Base = declarative_base()

class _Session(ABC):
    """Set up and destroy a database and session with proper resource management.

    This class provides a context manager interface for safe database session
    handling. It ensures proper cleanup of database resources and provides
    methods for session recreation when needed.

    Parameters
    ----------
    url : str
        The database URL to connect to. This can be a SQLite file URL or any
        other SQLAlchemy supported database URL.
    testing : bool
        If True, the database will be dropped upon destruction. This is useful
        for testing purposes to ensure a clean state for each test run. If False,
        the database will not be dropped, allowing it to persist across runs.
    echo : bool, optional
        If True, enables SQL statement logging. Default is False.

    Examples
    --------
    >>> # Preferred usage with context manager
    >>> with TestSession() as session_manager:
    >>>     session = session_manager.session
    >>>     # Use session...
    >>> # Resources automatically cleaned up

    >>> # Manual management (ensure close() is called)
    >>> session_manager = TestSession()
    >>> try:
    >>>     session = session_manager.session
    >>>     # Use session...
    >>> finally:
    >>>     session_manager.close()
    """

    def __init__(self, url, testing, echo=False):
        """Initialization."""
        self.testing = testing
        self.db_url = url
        self.echo = echo
        self.engine = None
        self.session = None
        self._closed = False

        try:
            self._initialize_database()
        except Exception as e:
            logger.error(f"Failed to initialize database {self.db_url}: {e}")
            self.close()  # Cleanup on failure
            raise

    def _initialize_database(self):
        """Initialize database engine, create database if needed, and create session."""
        # Create the SQLAlchemy ORM engine.
        self.engine = create_engine(self.db_url, echo=self.echo)
        logger.debug(f"Created database engine {self.db_url}")

        # Create database if it doesn't exist (skip for in-memory SQLite)
        if not self.db_url.startswith("sqlite:///:memory:") and not database_exists(self.db_url):
            logger.debug(f"Database {self.db_url} does not exist. Creating...")
            create_database(self.db_url)
            logger.debug(f"Created database {self.db_url}.")
        else:
            logger.debug(f"Database {self.db_url} ready.")

        # Create all tables
        Base.metadata.create_all(self.engine)
        logger.debug(f"Ensuring all tables exist in {self.db_url}.")

        # Create session
        self.session = Session(self.engine, autoflush=True, autocommit=False)
        logger.debug(f"Opened database session {self.db_url}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with proper cleanup."""
        self.close()
        if self.testing:
            self.drop_database()

    def new_session(self):
        """Create a new session, closing the current one if it exists.

        This is useful when you need a fresh session after operations that
        might have left the session in an inconsistent state.

        Returns
        -------
        sqlalchemy.orm.Session
            A new database session
        """
        if self._closed:
            raise RuntimeError("Cannot create new session - SessionManager is closed")

        if self.session:
            self.session.close()

        self.session = Session(self.engine, autoflush=True, autocommit=False)
        logger.debug(f"Created new session for {self.db_url}")
        return self.session

    def __del__(self):
        """Destructor - provides backup cleanup but should not be relied upon.

        Warning: __del__ is unreliable in Python. Use context manager or explicit
        close() calls instead. This is only a safety net.
        """
        if not self._closed:
            logger.warning(f"SessionManager for {self.db_url} was not properly closed. "
                         "Use context manager or explicit close() for reliable cleanup.")
            try:
                self.close()
                if self.testing:
                    self.drop_database()
            except Exception as e:
                logger.error(f"Error during cleanup in __del__: {e}")

    def close(self):
        """Close the database session and dispose of the engine.

        This method is idempotent - it can be called multiple times safely.
        After calling this method, the SessionManager should not be used further.
        """
        if self._closed:
            return  # Already closed

        try:
            # Close session if it exists
            if hasattr(self, 'session') and self.session is not None:
                try:
                    self.session.close()
                    logger.debug(f"Closed session for {self.db_url}.")
                except Exception as e:
                    logger.error(f"Error closing session for {self.db_url}: {e}")
                finally:
                    self.session = None

            # Dispose of engine if it exists
            if hasattr(self, 'engine') and self.engine is not None:
                try:
                    self.engine.dispose()
                    logger.debug(f"Disposed of engine for {self.db_url}.")
                except Exception as e:
                    logger.error(f"Error disposing engine for {self.db_url}: {e}")
                finally:
                    self.engine = None

        finally:
            self._closed = True

    def drop_database(self):
        """Drop the database if it exists.

        Warning: This permanently destroys all data in the database.
        Only use for testing or when you're certain you want to delete everything.
        """
        if self.db_url.startswith("sqlite:///:memory:"):
            logger.debug("In-memory database will be dropped automatically.")
            return

        try:
            if database_exists(self.db_url):
                drop_database(self.db_url)
                logger.debug(f"Dropped database {self.db_url}.")
        except Exception as e:
            logger.error(f"Error dropping database {self.db_url}: {e}")
            raise

    @property
    def is_closed(self):
        """Check if the session manager has been closed."""
        return self._closed


class TestSession(_Session):
    """Set up an in-memory test database and session for testing.

    This class creates a SQLite in-memory database that exists only for the
    duration of the session. It's automatically cleaned up when the session
    is closed, making it ideal for unit tests that need isolation.

    The session is immediately available as .session attribute for use with
    unittest setUp/tearDown patterns.

    Parameters
    ----------
    echo : bool, optional
        If True, enables SQL statement logging for debugging. Default is False.

    Examples
    --------
    >>> # For unittest setUp/tearDown pattern
    >>> def setUp(self):
    >>>     self.test_session = TestSession()
    >>>     self.session = self.test_session.session
    >>>
    >>> def tearDown(self):
    >>>     del self.test_session  # Automatic cleanup
    >>>
    >>> # For fresh session when needed (e.g., after deletes)
    >>> def test_something(self):
    >>>     # ... do some operations ...
    >>>     self.session = self.test_session.new_session()  # Fresh session
    """

    _URL = "sqlite://"  # In-memory SQLite database

    def __init__(self, echo=False):
        # Always testing=True for test sessions since in-memory DB is ephemeral
        super().__init__(self._URL, testing=True, echo=echo)


class SQLiteSession(_Session):
    """Set up a file-based SQLite database and session.

    This creates a persistent SQLite database file that survives across
    application runs. Useful for development and production environments.

    Parameters
    ----------
    testing : bool, optional
        If True, creates a test database that will be dropped when closed.
        If False (default), creates a persistent database. Default is False.
    echo : bool, optional
        If True, enables SQL statement logging for debugging. Default is False.

    Examples
    --------
    >>> # Production usage - persistent database
    >>> with SQLiteSession() as session_manager:
    >>>     session = session_manager.session
    >>>     # Work with persistent data...

    >>> # Testing usage - temporary database
    >>> with SQLiteSession(testing=True) as session_manager:
    >>>     session = session_manager.session
    >>>     # Database will be deleted on exit...
    """

    _DB_NAME = "asset_base"

    def __init__(self, testing=False, echo=False):
        # Construct SQLite file name with path expansion for a URL
        self._db_name = f"{self._DB_NAME}.db"

        # Put files in a `cache` folder under the `var` path scheme.
        db_path = get_cache_path(self._db_name, testing=testing)
        db_url = f"sqlite:///{db_path}"

        super().__init__(db_url, testing=testing, echo=echo)


class Common(Base):
    """Common object.

    This is the common base class Assets and Entities. As they and their child
    classes inherit from this class, they will share the common ``id`` and
    ``name`` attributes forcing uniqueness of id and name across both Assets and
    Entities which is a primary feature of this financial database system
    allowing entities to own assets.

    """

    __tablename__ = "common"

    # Polymorphism discriminator.
    _discriminator = Column(String(32))

    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
        "polymorphic_on": _discriminator,
    }

    _id = Column(Integer, primary_key=True, autoincrement=True)
    """ Primary key."""


    name = Column(String(256), nullable=False)
    """str: Entity name."""

    # Each child class must ensure uniqueness of name within its type.
    __table_args__ = (UniqueConstraint("_discriminator", "name"),)

    KEY_CODE_LABEL = "key_code"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes when joining."""

    # Entity dates.
    date_create = Column(Date, nullable=False)
    """sqlalchemy.DateTime: The creation date of the instance."""

    date_mod_stamp = Column(Date, nullable=True)
    """sqlalchemy.DateTime: Modification date stamp. May be in the past."""

    def __init__(self, name, **kwargs):
        """Instance initialization."""
        self.name = name

        # Record creation date
        self.date_create = datetime.datetime.today()

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return "{} - {}".format(self._id, self.name)

    def __repr__(self):
        """Return the official string output."""
        return '{}(name="{}", id={!r})'.format(self.__class__.__name__, self.name, self._id)

    @property
    def class_name(self):
        """Single word class name (not the full module path)."""
        return self.__class__.__name__

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return "{}.{}".format(self._id, self.name)

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return "{}.{}".format(self._id, self.name)

    @classmethod
    def key_code_id_table(cls, session):
        """A table of all instance's ``Common._id`` against ``key_code``.

        This table is useful for translating any other party's unique entity
        code keys to ``Common._id`` numbers, especially if the other party names
        their column the same as the ``entity_code_name`` attribute

        Returns
        -------
        pandas.DataFrame
            One row per instance in the database with two columns:
            - The first column shall be named ``id`` and contain the instance's
              ``Common._id`` number.
            - The second column shall be named after the class's
              ``KEY_CODE_LABEL`` attribute and contain the instance's
              ``key_code`` property value.
        """
        instances_list = session.query(cls).all()
        return pd.DataFrame(
            [(item._id, item.key_code) for item in instances_list],
            columns=["id", cls.KEY_CODE_LABEL],
        )

    @classmethod
    def factory(cls, session, **kwargs):
        raise NotImplementedError("This method must be overridden.")

    @classmethod
    def from_data_frame(cls, session, data_frame):
        """Create multiple class instances in the session from a dataframe.

        The instances are added to the session.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        data_frame : pandas.DataFrame
            A ``pandas.DataFrame`` with columns of the same name as all the
            class' ``factory`` method arguments, with the exception of the
            ``cls``, ``session`` and ``create`` arguments.

        """
        if data_frame.empty:
            return

        for i, row in data_frame.iterrows():
            cls.factory(session, **row)

    @classmethod
    def to_data_frame(cls, session):
        """Convert class data attributes into a factory compatible dataframe.

        The dataframe is compatible with the ``from_data_frame`` method.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.

        Returns
        -------
        data_frame : pandas.DataFrame
            A ``pandas.DataFrame`` with columns of the same name as the class'
            ``factory`` method argument names, with the exception of the
            ``cls``, ``session`` and ``create`` arguments.
        """
        record_list = list()
        # For all class instances in the database
        for instance in session.query(cls).all():
            # Get instance data dictionary and add the `Listed` ISIN number
            instance_dict = instance.to_dict()
            record_list.append(instance_dict)
        data_frame = pd.DataFrame(record_list)

        return data_frame

    @classmethod
    def update_all(cls, session, get_method, **kwargs):
        """Update/create all the objects in the asset_base session.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        get_method : class method
            The method that returns a ``pandas.DataFrame`` with columns of the
            same name as all the `factory` method arguments, with the exception
            of the `session` argument.
        kwargs : key work arguments
            Any parameters are passed to the ``get_method``.

        No object shall be destroyed, only updated, or missing object created.

        """
        # Get all financial data
        data_frame = get_method(**kwargs)
        # Bulk add/update data (uses the factory method)
        cls.from_data_frame(session, data_frame)
