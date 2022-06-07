#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Fund models data files.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the fundmanage module.
The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element

from asset_base.entitybase import Model
from asset_base.entitybase import Listed

# For abstract base classes.
import abc

import os

# Get module-named logger.
import logging
logger = logging.getLogger(__name__)


class _Feed(object):
    """Generic model feed data class.

    Parameters
    ----------
    entitybase : .entitybase.EntityBase
        An ``entitybase`` database manager with a session to the database.
    """

    # For abstract base classes.
    __metaclass__ = abc.ABCMeta

    def __init__(self, entitybase):
        """Instance initialization."""
        self.entitybase = entitybase
        self.session = entitybase.session

        self._path = os.path.dirname(
            os.path.abspath(__file__)) + self.data_path

    @abc.abstractmethod
    def _fetch(self):
        """Fetch data.

        Implementation left to the child class.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _parse(self):
        """Parse XML data.

        Implementation left to the child class.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def fetch(self):
        """Get models from data source.

        Implementation left to the child class.
        """
        raise NotImplementedError


class Sunstrike(_Feed):
    """Sunstrike Capital XML model data fetch class.

    Allows fetching from file of investment asset allocation models and storing
    them in the entity database.

    Parameters
    ----------
    entitybase : .entitybase.EntityBase
        An instance of the entitybase database.

    Example
    -------
    >>> from fundmanage.entitybase import EntityBase, Model
    >>> from fundmanage.model_feed import Sunstrike
    >>> entitybase = EntityBase()
    >>> model_feed = Sunstrike(entitybase)
    >>> model_feed.fetch()
    >>> # Fetch a model instance.
    >>> model = Model.factory(entitybase.session, ticker='ITRSVPCAU')
    >>> print '%r' % model
    <Model:Sunstrike Capital (Pty) Ltd:ITRSVPCAU>
    >>> # Fetch a model copy.
    >>> model = Model.factory(entitybase.session, ticker='ITRLAPCAU')
    >>> print '%r' % model
    <Model:Sunstrike Capital (Pty) Ltd:ITRLAPCAU>

    """

    data_path = '/data/models/'
    file_name = 'sunstrike_capital.xml'
    sub_path = ''

    def __init__(self, entitybase):
        """Instance initialization."""
        super(Sunstrike, self).__init__(entitybase)

    def _parse(self, tree):
        """Parse an XML tree."""

        # TODO: Use module defined exceptions classes instead of assert statements

        def get_parameter_dict(instance):
            parameter_dict = dict()
            #  Find the parameters.
            parameters = instance.findall('parameters')
            #  Get the parameters or return empty dict
            if len(parameters) == 0:
                return parameter_dict
            # Assert only one instance.
            assert len(parameters) == 1, "Expected only one parameters section."
            # Assemble the dict
            for item in parameters[0]:
                # Check structure.
                assert(item.tag == 'item')
                # Check for required attributes.
                assert('name' in item.attrib)
                # Add to dictionary.
                parameter_dict[item.attrib['name']] = item.text

            return parameter_dict

        def get_weight_dict(instance):
            weight_dict = dict()
            #  Get the weights or return empty dict
            weights = instance.findall('weights')
            if len(weights) == 0:
                return weight_dict
            # Assert only one or none instances.
            assert len(weights) == 1, "Expected only one weights section."
            # Assemble the dict
            for item in weights[0]:
                # Determine what variant of weight specification is being used.
                if item.tag == 'listed':
                    try:
                        asset = Listed.factory(self.session, create=False,
                                               **item.attrib)
                    except Exception as ex:
                        msg = 'Security not found: Model %s, Security %s' % \
                            (instance.attrib['ticker'], item.attrib['ticker'])
                        raise Exception(msg)
                else:
                    msg = ('Unexpected weight tag "%s" in model "%s"' %
                           (item.tag, instance.attrib['name']))
                    raise ValueError(msg)

                # Add weight to a dictionary with the entitybase.Asset.id number
                # as the key.
                weight_dict[asset.id] = float(item.text)

            return weight_dict

        # Get the XML tree root.
        root = tree.getroot()

        # Check structure. The <models> tag is the root.
        assert(root.tag == 'models')

        #  Add all model instances.
        for instance in root.iter('instance'):
            # Check instance attributes.
            assert('ticker' in instance.attrib)
            assert('name' in instance.attrib)
            assert('issuer' in instance.attrib)
            assert('domicile' in instance.attrib)

            # Get the parameters
            parameter_dict = get_parameter_dict(instance)
            assert len(parameter_dict) > 0, "Expected model parameters."

            # Get the weights
            weight_dict = get_weight_dict(instance)
            assert len(weight_dict) > 0, "Expected model weights."

            # Create as model in the session
            Model.factory(self.session,
                          instance.attrib['ticker'],
                          instance.attrib['issuer'],
                          instance.attrib['domicile'],
                          instance.attrib['name'],
                          weights=weight_dict,
                          parameters=parameter_dict,
                          )

        #  Add all model copies. Copies inherit the parent model and may
        #  overwrite with new parameters.
        for instance in root.iter('copy'):
            # Check instance attributes.
            assert('ticker' in instance.attrib)
            assert('name' in instance.attrib)
            assert('issuer' in instance.attrib)
            assert('domicile' in instance.attrib)

            # Get the parent. Assert only one instance.
            parent = instance.findall('parent')
            assert(len(parent) == 1)
            parent = parent[0]
            assert('ticker' in parent.attrib)
            assert('issuer' in parent.attrib)
            assert('domicile' in parent.attrib)

            # Retrieve the parent model.
            try:
                parent_model = Model.factory(
                    self.session,
                          parent.attrib['ticker'],
                          parent.attrib['issuer'],
                          parent.attrib['domicile'],
                    )
            except Exception:
                msg = ('Failed creating Model copy %s. '
                       'Parent Model %s not found in session.') % \
                    (instance.attrib['ticker'],
                     parent.attrib['ticker'])
                logger.error(msg)

            # Get the parameters, if any then update, or add to, the parent's
            parent_parameter_dict = parent_model.get_parameter_dict()
            parameter_dict = get_parameter_dict(instance)
            if len(parameter_dict) > 0:
                parent_parameter_dict.update(parameter_dict)

            # Get the weights, if any then update, or add to, the parent's
            parent_weight_dict = parent_model.get_weight_dict()
            weight_dict = get_weight_dict(instance)
            if len(weight_dict) > 0:
                parent_weight_dict.update(weight_dict)

            # Create the copy model in the session from the parent
            # information.
            Model.factory(self.session,
                            instance.attrib['ticker'],
                            instance.attrib['issuer'],
                            instance.attrib['domicile'],
                            instance.attrib['name'],
                            parameters=parent_parameter_dict,
                            weights=parent_weight_dict,
                            )

    def _fetch(self, path):
        """Fetch data from and XML file."""
        return ET.parse(path)

    def fetch(self, file_name=None):
        """Fetch models from the local file.

        Parameters
        ----------
        file_name : str
            A full path specification to the models XML file.


        This method reads a file at the relative package path ::

            ./data/models/sunstrike_capital.xml

        which has the form in the example below:

        .. code-block:: xml

            <?xml version="1.0" encoding="UTF-8"?>
            <models>
              <instance ticker='ITRSVPCAU'>
                <details name='Itransact Cautious Savings Portfolio'
                  issuer='Sunstrike Capital (Pty) Ltd' domicile='ZA'/>
                <parameters>
                  <item name='cash_minimums_default_proportional'>0.02</item>
                  <item name='cash_minimums_default_absolute'>5.0</item>
                  <item name='cash_minimums_selling_proportional'>0.05</item>
                  <item name='cash_minimums_selling_absolute'>10.0</item>
                  <item name='weight_tolerance'>0.20</item>
                  <item name='turnover_limit'>0.30</item>
                </parameters>
                <weights>
                  <listed ticker='DBXUS' mic='XJSE'>0.15</listed>
                  <listed ticker='GIVISA' mic='XJSE'>0.07</listed>
                  <listed ticker='NFILBI' mic='XJSE'>0.35</listed>
                  <listed ticker='NFTRCI' mic='XJSE'>0.25</listed>
                  <listed ticker='STPROP' mic='XJSE'>0.18</listed>
                </weights>
              </instance>
              <copy ticker='ITRLAPCAU' parent_ticker='ITRSVPCAU'>
                <details name='Itransact Cautious Living Annuity'
                  issuer='Sunstrike Capital (Pty) Ltd' domicile='ZA'/>
              </copy>
            </models>

        The ``models`` tag is required. Any models are specified by the
        ``instance`` tag which must have a ``parameters`` and ``weights`` tag.

        A ``parameters`` tag may contain any number of ``item`` tags of
        arbitrary ``name`` attribute and with arbitrary value strings.

        A ``listed`` tag must have a ``ticker`` and ``mic`` attribute with a
        valid exchange ticker and MIC exchange code. Instead of these two
        attributes  it may have only an ``isin`` attribute with a valid ISIN
        number.

        """
        # FIXME: Feeds should have the final session.rollback()/commit(), not the database modules.
        # This will give feeds the decision that its okay to commit or rollback.

        if file_name is None:
            # Use the built in data path.
            path = self._path + self.sub_path + self.file_name
        else:
            path = file_name

        tree = self._fetch(path)
        self._parse(tree)


class TestData(Sunstrike):
    """Sunstrike Capital XML model test data fetch class."""

    data_path = '/tests/'
    file_name = 'data_sunstrike_models.xml'
    sub_path = ''


class ModelFeed(object):
    """Manage model data feeds and data.

    Parameters
    ----------
    entitybase : .entitybase.EntityBase
        An ``entitybase`` database manager with a session to the database.

    See also
    --------
    .entitybase.Model,
    .entitybase.EntityBase,
    """

    def __init__(self, entitybase):
        """Initialize instance."""
        self.entitybase = entitybase
        self.session = entitybase.session

    def add_model(self, source):
        """Insert data from a source into the session.

        Parameters
        ----------
        source : str
            The source of the data:

            'testdata':
                Local test data XML file.
            'sunstrike_capital':
                Sunstrike Capital models XML file.

        """
        if source == 'testdata':
            logger.info('Fetching test model data in XML format.')
            source = TestData(self.entitybase)
            source.fetch()
        elif source == 'sunstrike_capital':
            logger.info('Fetching Sunstrike Capital model data in XML format.')
            source = Sunstrike(self.entitybase)
            source.fetch()
        else:
            raise ValueError('Unexpected source argument.')

        self.session.commit()
