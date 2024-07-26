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
    """Set up and destroy a database and session."""

    def __init__(self, url, testing):
        """Initialization."""
        self.testing = testing
        self.db_url = url

        self.engine = create_engine(self.db_url, echo=False)  # No logging
        logger.info(f"Created database engine {self.db_url}")

        Base.metadata.create_all(self.engine)  # Using asset_base.Base
        logger.info(f"Created all tables in {self.db_url}.")

        self.session = Session(self.engine, autoflush=True, autocommit=False)
        logger.info(f"Opened database session {self.db_url}")

    def __del__(self):
        """Destruction."""
        # Delete database if it exists
        if not database_exists(self.db_url):
            return
        # Properly close the session and dispose of the engine
        self.session.close()
        del self.session
        logger.info(f"Closed database session {self.db_url}.")
        self.engine.dispose()
        del self.engine
        logger.info(f"Disposed of database engine {self.db_url}.")
        # Only delete database if we are testing - otherwise keep it.
        if self.testing is True:
            drop_database(self.db_url)
            logger.warning(f"Dropped the entire database {self.db_url}.")


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
