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
from sqlalchemy_utils import drop_database, database_exists

from asset_base import get_cache_path


logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Create the declarative base
Base = declarative_base()

class _Session(ABC):
    """Set up and destroy a database and session.

    Parameters
    ----------
    url : str
        The database URL to connect to. This can be a SQLite file URL or any
        other SQLAlchemy supported database URL.
    testing : bool
        If True, the database will be dropped upon destruction. This is useful
        for testing purposes to ensure a clean state for each test run. If False,
        the database will not be dropped, allowing it to persist across runs.

    """

    def __init__(self, url, testing):
        """Initialization."""
        self.testing = testing
        self.db_url = url

        # Create the SQLAlchemy ORM engine.
        # Set echo=False to disable logging of SQL statements.
        # Set echo=True to enable logging of SQL statements.
        self.engine = create_engine(self.db_url, echo=False)  # No logging
        logger.debug(f"Created database engine {self.db_url}")

        # Create all the database tables using the declarative_base defined
        # above.
        # FIXME: Handle edge cases where the database already exists.
        Base.metadata.create_all(self.engine)
        logger.debug(f"Created all tables in {self.db_url}.")

        # Create a new session for the database. Set autoflush=True to flush
        # changes to the database before each query. Set autocommit=False to
        # disable autocommit mode. This means that changes are not committed to
        # the database until explicitly called with session.commit(). This is
        # useful for ensuring that all changes are made in a single transaction,
        # which can be rolled back if needed. If autoflush is set to False, you
        # need to call session.flush() before querying the database to ensure
        # that all changes are flushed to the database. The "autocommit" keyword
        # is present for backwards compatibility but must remain at its default
        # value of False.
        self.session = Session(self.engine, autoflush=True, autocommit=False)
        logger.debug(f"Opened database session {self.db_url}")

    def __del__(self):
        """Destruction.

        This method is called when the object is about to be destroyed. This
        will ensure that the database session and engine are properly closed and
        disposed of.


        If the `testing` flag is set to True (See class testing parameter), the
        database will be dropped to ensure a clean state for the next test run.
        If False, the database will not be dropped, allowing it to persist
        across runs.
        """
        if self.testing:
            logger.debug(f"Deleting database {self.db_url} because we are testing.")
            self.close(drop=True)
        else:
            self.close(drop=False)

    def close(self, drop=False):
        """Close the database session and dispose of the engine.

        Parameters
        ----------
        drop : bool, optional
            If True, the database will be dropped. This is useful for testing
            purposes to ensure a clean state for the next test run. If False,
            the database will not be dropped, allowing it to persist across runs.

        """
        # Delete database session and engine only if the database exists. This
        # guard is important as the database may not exist when __del__ is
        # called, due to previous calls to close() or when testing.
        if not database_exists(self.db_url):
            logger.debug(f"Database {self.db_url} does not exist. Nothing to delete.")
            return

        # If the session exists, close it and dispose of the engine.
        if hasattr(self, 'session') and self.session is not None:
            # Close the session to release any resources it holds. This is
            # important to ensure that the session is properly cleaned up and
            # does not hold onto any database connections or resources. This is
            # especially important in a testing environment where the database
            # may be dropped and recreated frequently. Closing the session will
            # also ensure that any pending changes are flushed to the database
            # before the session is closed. This is important to ensure that any
            # changes made to the database are properly saved before the session
            # is closed.
            self.session.close()
            del self.session  # Delete the session attribute
            logger.debug(f"Closed session for {self.db_url}.")
            # Dispose of the engine to release any resources it holds.
            self.engine.dispose()
            del self.engine  # Delete the engine attribute
            logger.debug(f"Disposed of engine for {self.db_url}.")

        if drop is True:
            # If we are not testing, just close the session and engine.
            drop_database(self.db_url)
            logger.debug(f"Dropped database for {self.db_url}.")


class TestSession(_Session):
    """Set up an `in-memory` test database and session."""

    _URL = "sqlite://"

    def __init__(self):
        # Default is testing is True
        super().__init__(self._URL, testing=True)


class SQLiteSession(_Session):
    """Set up an `in-memory` test database and session."""

    _DB_NAME = "asset_base"

    def __init__(self, testing=False):
        # Construct SQLite file name with path expansion for a URL
        self._db_name = "%s.db" % self._DB_NAME

        # Put files in a `cache`` folder under the `var` path scheme.
        db_path = get_cache_path(self._db_name, testing=testing)
        db_url = "sqlite:///" + db_path

        super().__init__(db_url, testing=testing)




class Common(Base):
    """Common object."""

    __tablename__ = "common"

    # Polymorphism discriminator.
    _discriminator = Column(String(32))

    __mapper_args__ = {
        "polymorphic_identity": __tablename__,
        "polymorphic_on": _discriminator,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    """ Primary key."""

    __table_args__ = (UniqueConstraint("_discriminator", "id"),)

    name = Column(String(256), nullable=False)
    """str: Entity name."""

    key_code_name = "key_code"
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

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
        return "{} - {}".format(self.id, self.name)

    def __repr__(self):
        """Return the official string output."""
        return '{}(name="{}", id={!r})'.format(self._class_name, self.name, self.id)

    @classmethod
    @property
    def _class_name(cls):
        return cls.__name__

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return "{}.{}".format(self.id, self.name)

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return "{}.{}".format(self.id, self.name)

    @classmethod
    def key_code_id_table(cls, session):
        """A table of all instance's ``Entity.id`` against ``key_code``.

        This table is useful for translating any other party's unique entity
        code keys to ``Entity.id`` numbers, especially if the other party names
        their column the same as the ``entity_code_name`` attribute

        Returns
        -------
        pandas.DataFrame
            The key code column name shall be the class' ``key_code_name``
            attribute.
        """
        instances_list = session.query(cls).all()
        return pd.DataFrame(
            [(item.id, item.key_code) for item in instances_list],
            columns=["id", cls.key_code_name],
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
