#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

""" Define classes for fund asset allocation models
"""

import sys

from sqlalchemy import Integer, String
from sqlalchemy import MetaData, Column, ForeignKey

from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.ext.declarative import declarative_base

from asset_base.exceptions import FactoryError, ReconcileError, ReconcileError
from asset_base.entity import Domicile, Entity, Issuer

# Get module-named logger.
import logging
logger = logging.getLogger(__name__)
# Change logging level here.
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Create the declarative base
Base = declarative_base()

# Pull in the meta data
metadata = MetaData()


class Model(Entity):
    """Provide asset allocation model weights for a fund.

    A model is an ideal asset allocation for a fund issued by an issuer with a
    ticker unique to that issuer. The asset allocation is in terms of a set of
    `Asset` instances and their weights which must sum to unity.

    Parameters
    ----------
    name : str
        Model full name.
    issuer: .Issuer
        The issuing institution that issued the model.
    ticker : str
        A short mnemonic code (often derived from the name) used to identity
        the issuer's model. Tickers must be unique within models for a single
        issuer. Models form different issuers may have the same ticker.
    weights : list
        A list of `EntityWeight` instances.  See the `Entity` and `EntityWeight`
        documentation especially the parameter `children`.
    parameters : list, optional
        A list of `ModelParameter` instances. For capturing parameters extra to
        the model weights that affect how the model is used.

    Attributes
    ----------
    issuer : .Issuer
        The issuing institution that issued the model.
    ticker : str
        A short mnemonic code (often derived from the name) used to identity
        the issuer's model. Tickers must be unique within models for a single
        issuer. Models form different issuers may have the same ticker.
    issue_date : datetime.date
        The date the latest model was created in the database.

    See also
    --------
    .Entity, .Issuer, .EntityWeight

    """

    __tablename__ = 'model'
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    # Entity.id
    id = Column(Integer, ForeignKey('entity.id'), primary_key=True)

    # Issuer id.
    issuer_id = Column(Integer, ForeignKey('issuer.id'), nullable=False)

    # Collection of ModelParameter instances
    _parameters = relationship('ModelParameter', cascade='all, delete-orphan')

    # Model ticker.
    ticker = Column(String(12), nullable=False)

    #  A short class name for use in the alt_name method.
    _class_name = 'Model'

    def __init__(self, name, issuer, ticker, **kwargs):
        """Instance initialization."""

        # Constrain domicile to that of the issuer.
        domicile = issuer.domicile

        # NOTE: I got an "IntegrityError: (raised as a result of Query-invoked
        # autoflush; consider using a session.no_autoflush block if this flush
        # is occurring prematurely)" as a result of placing these lines before
        # the "domicile = issuer.domicile" line above. It appears that one
        # should first query before setting attributes or and autoflush may be
        # triggered.
        self.ticker = ticker
        self.issuer = issuer

        super().__init__(name, domicile, **kwargs)

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return 'Model %s (%s) issued by %s in %s' % \
            (self.name, self.ticker, self.issuer.name, self.domicile.name)

    @property
    def key_code(self):
        """A key string unique to the class instance."""
        return self.issuer.key_code + '.' + self.ticker

    @property
    def identity_code(self):
        """A human readable string unique to the class instance."""
        return self.issuer.identity_code + '.' + self.ticker

    @property
    def securities(self):
        """Return the list of model security instances."""
        weights = self.get_weights()
        return [entity for entity, weight in weights]

    def get_weight_dict(self):
        """Return a weight dictionary for the model.

        Returns
        -------
        dict
            Dictionary of weights with the ``Entity.id`` as key.

        See also
        --------
        .Entity.get_weights
        """
        # FIXME:Gets leaves weights so won't work for hierarchies of holdings.
        weights = self.get_weights()
        return dict([(entity.id, weight) for entity, weight in weights])

    def get_parameter_dict(self):
        """Return a parameter dictionary for the model.

        Returns
        -------
        dict
            Dictionary of parameter strings with the parameter names as key.

        """
        return dict([(item.name, item.value)
                     for item in self._parameters])

    def _add_parameters(self, session, parameter_list):
        """Add ModelParameter instances for each parameter in the list.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        parameter_list : list
            List of (entity : Entity, weight : float) tuples.

        """
        # First delete any existing instance parameters from the session.
        for parameter in self._parameters:
            session.delete(parameter)
        session.flush()

        # Create a list of ModelParameter instances from the parameter_list.
        parameters = list()
        for name, value in parameter_list:
            parameters.append(ModelParameter(name, value))
        self._parameters = parameters
        session.flush()

    @classmethod
    def factory(cls, session, ticker, issuer_name, issuer_domicile_code,
                model_name=None,
                **kwargs):
        """Manufacture/retrieve an instance from the given parameters.

        If a record of the specified class instance does not exist then add it,
        else do only return the instance.

        Note
        ----
        The entity's domicile implies the currency.

        Note
        ----
        For models the issuer domicile and the model domicile are constrained to
        be the same.

        Note
        ----
        If the instance already exists in the session then any ``parameters``
        and ``weights`` arguments are ignored. Only new models will use these
        arguments. If new ``parameters`` or ``weights`` are to be specified then
        the model instance in question must itself first be erased for the
        session and a new one created with the new ``parameters`` and
        ``weights`` arguments. Typically the ``EntityBase.set_up()`` and
        ``EntityBase.tear_down()`` methods erase the entire entitybase database.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            A session attached to the desired database.
        ticker : str
            A short mnemonic code (often derived from the name) used to identity
            the model.
        issuer_name : str
            The name of the issuer institution that issued the share.
        issuer_domicile_code : str(2)
            ISO 3166-1 Alpha-2 two letter country code for the issuer domicile.
        model_name : str, optional
            Entity full name.
        weights : dict, optional
            A dictionary of entity holding weights. The specified child `Entity`
            instances must already exist in the session or an exception shall be
            raised.

            Build the dictionary as follows::

                weights = {
                    id1: weight1,
                    ...,
                    idN: weightN,
                }

            where id is a `.Entity.id` number and weight1
            to weightN are float numbers. IN a typical fund the weights sum to
            1.0.

        parameters : dict, optional
            A dictionary of model parameters. For capturing parameters extra to
            the model weights that affect how the model is used.

            Build the dictionary as follows::

                parameters = {
                    name1: value1,
                    ...,
                    nameN: valueN,
                }

            where name1 to nameN are the parameter names and value to valueN are
            the string representation of the parameter value.

        create : bool, optional
            If `False` then the factory shall expect the specified `Entity` to
            exist in the session or it shall raise an exception.
        date_stamp : datetime.datetime, optional
            The date stamp of the data in the above parameters. If provided and
            the instance already exists in the session then the
            ``Entity.date_mod_stamp`` is checked and the reconcile operation is
            skipped if the ``date_stamp`` is not newer. This is to prevent
            reconcile operations against existing instances with stale data.

        Return
        ------
        .Entity
            The single instance that is in the session.

        See also
        --------
        .Entity.factory, Issuer.factory

        """
        # Get issuer. Note that we shall create an issuer if it is not in the
        # session.
        issuer = Issuer.factory(session, issuer_name, issuer_domicile_code)

        # Check if entity exists in the session and if not then add it.
        try:
            obj = session.query(cls).join(Domicile).join(Issuer).filter(
                cls.ticker == ticker,
                Issuer.name == issuer_name,
                Domicile.code == issuer_domicile_code
            ).one()
            obj._reconcile(ticker, issuer_name, issuer_domicile_code,
                           model_name, **kwargs)
        except NoResultFound:
            if 'create' in kwargs and kwargs['create'] is False:
                raise FactoryError('Model, ticker=%s, issuer=%s, not found.' %
                                   (ticker, issuer))
            # Create.
            obj = cls(model_name, issuer, ticker, **kwargs)
            session.add(obj)

            #  Check for model parameters.
            if 'parameters' in kwargs:
                # Create model parameters.
                parameter_list = list()
                for name, value in kwargs['parameters'].items():
                    parameter_list.append((name, value))
                obj._add_parameters(session, parameter_list)

            #  Check for children entities and their weights in keyword
            #  arguments.
            if 'weights' in kwargs:
                # Find all the list of child Entity instances.
                children_list = list()
                for id, weight in kwargs['weights'].items():
                    # Call factory. Disallow creation of new entities.
                    child = session.query(Entity).filter(Entity.id == id).one()
                    children_list.append((child, weight))
                #  Add the list of children as `EntityWeight` instances.
                obj._add_entity_weights(session, children_list)

        else:
            # Reconciliation of any changes.
            cls._reconcile(
                ticker, issuer_name, issuer_domicile_code, model_name)
        finally:
            session.flush()

        return obj

    def _reconcile(self, ticker,
                   issuer_name, issuer_domicile_code,
                   model_name=None,
                   **kwargs):
        """Reconcile specified parameters with class instance attributes.

        The parameters are the same as those for the `factory` method with the
        exception of the `session` argument. This method is always used by the
        `factory` method to reconcile instances retrieved from the session.

        Raises
        ------
        ReconcileError
            The specified parameters do not reconcile with class instance
            attributes.
        """
        # Local attributes to reconcile. Don't use superclass reconcile as
        # domicile is already constrained to that of the issuer.
        if model_name and model_name != self.name:
            raise ReconcileError(self, 'model_name')
        if ticker and ticker != self.ticker:
            raise ReconcileError(self, 'ticker')

        # Reconcile issuer.
        if issuer_name and issuer_domicile_code:
            self.issuer._reconcile(issuer_name, issuer_domicile_code)
        elif issuer_name and issuer_domicile_code is None:
            raise Exception(
                'Require both issuer_name, issuer_domicile_code arguments.')
        elif issuer_name is None and issuer_domicile_code:
            raise Exception(
                'Require both issuer_name, issuer_domicile_code arguments.')
        else:
            pass

        #  Reconcile children.
        self._reconcile_children(**kwargs)


class ModelParameter(Base):
    """Parameters strings of a model.

    Stored as string names with associated string values.

    Parameters
    ----------
    name : string
        The parameter by name.
    value : string
        The parameter value in string representation.

    Attributes
    ----------
    name : string
        The parameter by name.
    value : string
        The parameter value in string representation.

    See Also
    --------
    .Model

    """

    __tablename__ = 'model_param'

    # Owning model.
    _model_id = Column(Integer, ForeignKey('model.id'), primary_key=True)

    # Parameter name string.
    name = Column(String(32), primary_key=True)

    # Parameter value string.
    value = Column(String(32), nullable=False)

    # Relationship to model.
    # FIXME: Move relationship to Model side.
    _model = relationship('Model', back_populates='_parameters')

    def __init__(self, name, value):
        """Instance initialization."""
        self.name = name
        self.value = value

    def __str__(self):
        """Return the informal string output. Interchangeable with str(x)."""
        return 'Model parameter "%s" = "%s"' % (self.name, self.value)

