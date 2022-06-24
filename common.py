#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Declare common object infrastructure
"""

import sys
import datetime

import pandas as pd

from sqlalchemy import Integer, String, Date
from sqlalchemy import Column
from sqlalchemy import UniqueConstraint

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import Session
from sqlalchemy import create_engine

# Get module-named logger.
import logging
logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Create the declarative base
Base = declarative_base()


class TestSession(object):
    """Set up a test database and session."""

    def __init__(self):
        self.engine = create_engine('sqlite://', echo=True)
        Base.metadata.create_all(self.engine)  # Using asset_base.Base
        self.session = Session(self.engine, autoflush=True, autocommit=False)


class Common(Base):
    """ Common object.
    """

    __tablename__ = 'common'

    # Polymorphism discriminator.
    _discriminator = Column(String(32))

    id = Column(Integer, primary_key=True, autoincrement=True)
    """ Primary key."""

    name = Column(String(256), nullable=False)
    """str: Entity name."""

    # Is being depreciated
    _alt_name = Column(String(256), nullable=True)

    key_code_name = 'key_code'
    """str: The name to attach to the ``key_code`` attribute (@property method).
    Override in  sub-classes. This is used for example as the column name in
    tables of key codes."""

    # Entity dates.
    date_create = Column(Date, nullable=False)
    """sqlalchemy.DateTime: The creation date of the instance."""

    date_mod_stamp = Column(Date, nullable=True)
    """sqlalchemy.DateTime: Modification date stamp. May be in the past."""

    __mapper_args__ = {
        'polymorphic_identity': __tablename__,
        'polymorphic_on': _discriminator,
    }

    # In all polymorph cases the _discriminator must remain as the
    # `__tablename__` and in most polymorph cases the _discriminator together
    # with the `id` shall by inheritance be constrained to be unique. In
    # exceptions to this case the `UniqueConstraint` may be changed by
    # overriding the `__table_args__` attribute
    # TODO: Use multiple primary keys across tables instead
    __table_args__ = (UniqueConstraint('_discriminator', 'id'), )

    def __init__(self, name, **kwargs):
        """Instance initialization."""
        self.name = name
        # Record alternative name if exists.
        if 'alt_name' in kwargs:
            self._alt_name = kwargs.pop('alt_name')

        # Record creation date
        self.date_create = datetime.datetime.today()

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return '{} - {}'.format(self.id, self.name)

    def __repr__(self):
        """Return the official string output."""
        return '<{}(name="{}", id={!r})>'.format(
            self._class_name, self.name, self.id)

    @classmethod
    @property
    def _class_name(cls):
        return cls.__name__

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return '{}.{}'.format(self.id, self.name)

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return '{}.{}'.format(self.id, self.name)

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
            columns=['id', cls.key_code_name])

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
        """ Update/create all the objects in the asset_base session.

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

