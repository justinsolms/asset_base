#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Declare common object infrastructure
"""

import sys
import base64
import hashlib
import networkx
import datetime

import pandas as pd

from sqlalchemy import Float, Integer, String, Date
from sqlalchemy import Column
from sqlalchemy import UniqueConstraint

from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import Session
from sqlalchemy import create_engine


from asset_base.exceptions import FactoryError, HoldingsError, ReconcileError

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
        Base.metadata.create_all(self.engine)  # Using entitybase.Base
        self.session = Session(self.engine, autoflush=True, autocommit=False)


class Common(Base):
    """ Common object.
    """
    __tablename__ = 'common'
    __mapper_args__ = {'polymorphic_identity': __tablename__, }

    id = Column(Integer, primary_key=True, autoincrement=True)
    """ Primary key."""

    # Polymorphism discriminator.
    _discriminator = Column(String(32))

    # A string created by the class which when combined with the _discriminator
    # attribute shall constitute a unique constraint. It shall be up to the
    # class to implement this string based on what makes an instance of that
    # class unique. The string shall be a 32-character maximum.
    _key = Column(String(32))

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

    __table_args__ = (
        # The entity instance shall be unique in every polymorphic class by its
        # _key attribute. See above comments on _key attribute
        UniqueConstraint('_discriminator', '_key'),
    )

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.id

    def set_key(self):
        """Set the unique constraint's key attribute.

        The key is calculated from a hash of the ``key_code`` attribute.

        Note
        ----
        To be called at instance initialization and when a class attribute that
        defines the uniqueness of an instance is changed.
        """
        # This should call child class' key_code (polymorphism)
        key_string = self.key_code
        # Unicode-objects must be encoded before hashing
        key_string = key_string.encode('utf-8')
        hasher = hashlib.sha1(key_string)  # Collisions are unlikely, use short
        digest = hasher.digest()  # Digests look horrible
        self._key = base64.urlsafe_b64encode(digest)  # Make nice ASCII string

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
            columns=['entity_id', cls.key_code_name])

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
        for instance in session.query(cls).all():
            # Get instance data dictionary and add the `Listed` ISIN number
            instance_dict = instance.to_dict()
            record_list.append(instance_dict)
        data_frame = pd.DataFrame(record_list)

        return data_frame

    @classmethod
    def update_all(cls, session, get_method, **kwargs):
        """ Update/create all the objects in the entitybase session.

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

