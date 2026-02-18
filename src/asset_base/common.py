#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Declare common object infrastructure.

This module provides the foundational classes for the asset_base system,
including database session management and the base ``Common`` class that
all entities and assets inherit from.

Factory Method Paradigm
------------------------
All classes inheriting from ``Common`` implement a factory method pattern
with dual-mode behavior:

**Retrieval Mode** (minimal parameters):
    When only key identifying parameters are provided, the factory attempts
    to retrieve an existing instance from the database. If not found, raises
    ``FactoryError``.

    Example::

        currency = Currency.factory(session, ticker="USD")

**Creation Mode** (full parameters):
    When all required parameters are provided, the factory retrieves an
    existing instance if found, or creates a new one if missing.

    Example::

        currency = Currency.factory(
            session, ticker="USD", name="US Dollar",
            country_code_list=["US"]
        )

**Dependency Enforcement**:
    Higher-level classes call lower-level factories in retrieval mode to
    enforce that dependencies must pre-exist:

    - ``Currency`` (base level, no dependencies)
    - ``Domicile`` → requires ``Currency`` to exist
    - ``Entity``/``Exchange`` → require ``Domicile`` to exist

    This ensures referential integrity and prevents accidental creation of
    foundational data records.

See Also
--------
entity : Entity and related classes with factory methods
asset : Asset classes with factory methods
"""
from abc import ABC, ABCMeta, abstractmethod

import sys
import datetime
import logging

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy import Integer, String, Date, Column, UniqueConstraint
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm import declarative_base, Session, declared_attr, object_session
from sqlalchemy_utils import drop_database, database_exists, create_database  # type: ignore

from .financial_data import MetaData as FinancialMetaData

from asset_base import get_cache_path


logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


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


class UniqueNameMixin(object):
    """Mixin to ensure uniqueness of names within each subclass."""

    _id = Column(Integer, primary_key=True, autoincrement=True)
    _discriminator = Column(String(50))
    name = Column(String(256), nullable=False)

    __mapper_args__ = { "polymorphic_on": _discriminator, }

    @declared_attr
    def __table_args__(cls):
        # Applies to every mapped subclass that includes this mixin
        return (UniqueConstraint("_discriminator", "name"),)


class CombinedMeta(DeclarativeMeta, ABCMeta):
    """Create a combined metaclass inheriting from DeclarativeMeta and ABCMeta.

    This avoids metaclass conflicts when using abstract base classes with
    SQLAlchemy's declarative base such as:

        `TypeError: metaclass conflict: the metaclass of a derived class must be
        a (non-strict) subclass of the metaclasses of all its bases`

    Note
    ----
    Why This Works is when Python sees class Common(Base, ABC, ...), it needs to
    determine the metaclass:

    - Check Base → uses CombinedMeta
    - Check ABC → uses ABCMeta
    - Verify: Is CombinedMeta a subclass of ABCMeta? → Yes! ✓

    Since CombinedMeta inherits from ABCMeta (and DeclarativeMeta), there's no
    conflict. Python uses CombinedMeta for Common, which gives you:

    - SQLAlchemy's automatic table mapping behaviour
    - Python's abstract base class enforcement
    - All functionality from both!

    This is a common pattern when working with libraries that use custom
    metaclasses alongside Python's built-in abstract classes.
    """
    pass


# Create the declarative base with the combined metaclass
Base = declarative_base(metaclass=CombinedMeta)


class Common(Base):
    """Common object.

    This is the common base class Assets and Entities. As they and their child
    classes inherit from this class, they will share the common ``id`` and
    ``name`` attributes forcing uniqueness of id and name across both Assets and
    Entities which is a primary feature of this financial database system
    allowing entities to own assets.

    """

    __tablename__ = "common"

    # Primary key.
    _id = Column(Integer, primary_key=True, autoincrement=True)
    # Polymorphism discriminator.
    _discriminator = Column(String(32))
    # Common name
    name = Column(String(256), nullable=False)

    # Each child class must ensure uniqueness of name across all instances.
    __table_args__ = (UniqueConstraint("_discriminator", "name"),)

    __mapper_args__ = {
        "polymorphic_on": _discriminator,
        # no polymorphic_identity here; this class is effectively abstract
    }

    KEY_CODE_LABEL = "key_code"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes when joining."""

    # Entity dates.
    date_create = Column(Date, nullable=False)
    """sqlalchemy.DateTime: The creation date of the instance."""

    date_mod_stamp = Column(Date, nullable=True)
    """sqlalchemy.DateTime: Modification date stamp. May be in the past."""

    # The financial_data module metadata getter method provider instance for all
    # classes inheriting from Common.
    METADATA_INSTANCE = FinancialMetaData()

    # The metadata get method should be overridden in child classes to return
    # the appropriate metadata get method for that class. Use the METADATA
    # instance.
    METADATA_GET_METHOD = None

    def __init__(self, name):
        """Instance initialization."""
        self.name = name

        # Record creation date
        self.date_create = datetime.datetime.today()

    @abstractmethod
    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        pass

    @abstractmethod
    def __repr__(self):
        """Return the official string output."""
        pass

    @property
    @abstractmethod
    def key_code(self):
        """A key string unique to the class instance."""
        pass

    @property
    @abstractmethod
    def identity_code(self):
        """A human readable string unique to the class instance."""
        pass

    @property
    @abstractmethod
    def long_name(self):
        """str: Return the long name string."""
        pass

    @classmethod
    @abstractmethod
    def factory(cls, session, **kwargs):
        """Manufacture/retrieve an instance from the given parameters.

        Factory Method Behaviour
        ------------------------
        This abstract method defines the factory pattern used throughout the
        asset_base system. Implementations follow a dual-mode paradigm:

        **Retrieval Mode** (minimal parameters):
            Provide only key identifying parameters. Returns existing instance
            or raises ``FactoryError`` if not found.

        **Creation Mode** (full parameters):
            Provide all required parameters. Returns existing instance if found,
            creates new instance if missing.

        The ``create`` parameter (when present) can explicitly control behaviour:
            - ``create=False``: Force retrieval mode, raise error if not found
            - ``create=True``: Allow creation if instance doesn't exist (default)

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            The database session.
        **kwargs
            Class-specific parameters. See concrete implementations for details.

        Returns
        -------
        Common
            The single instance that is in the session.

        Raises
        ------
        FactoryError
            If instance not found in retrieval mode or if required dependencies
            don't exist.
        ReconcileError
            If provided parameters conflict with existing instance data.

        See Also
        --------
        Currency.factory : Base-level factory with no dependencies
        Domicile.factory : Requires Currency to exist
        Entity.factory : Requires Domicile to exist
        """
        pass

    @property
    def class_name(self):
        """Single word class name (not the full module path)."""
        return self.__class__.__name__

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
            # Call class factory method. Each class factory method should be
            # designed to handle creation of instances from the provided row
            # data.
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

