#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""The ``asset_base`` package initialization.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the fundmanage module.
The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
import sys

# read version from installed package
from importlib.metadata import version

import logging
import logging.config
import yaml
import os

# Package absolute root path
_ROOT = os.path.abspath(os.path.dirname(__file__))

# Data file parent path
_DATA = "data"

# Variable data path
_VAR = "var"
# Variable data path for tests - should always delete after tests!
_VAR_TEST = "var_test"


def get_data_path(sub_path):
    """Package path schema for fixed data

    Parameters
    ----------
    sub_path: str
        Mandatory branch or child path.
    """
    return os.path.join(_ROOT, _DATA, sub_path)


def get_var_path(sub_path, testing=False):
    """Package path schema for variable data such as logs and databases.

    Parameters
    ----------
    sub_path: str
        Mandatory branch or child path.
    testing : bool
        If `True` then the returned path string contains `/var_test/` instead of
        the default `/var/`.
    """
    if testing:
        path = os.path.join(_ROOT, _VAR_TEST, sub_path)
    else:
        path = os.path.join(_ROOT, _VAR, sub_path)
    return path


# Open logging configuration YAML file and convert ot a dict.
path = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(path, "logconf.yaml"), "r") as stream:
    config = yaml.full_load(stream)
# Use the dict to configure logging.
logging.config.dictConfig(config)

# Record the current version and log it.
# __version__ = version("asset_base")  # FIXME: importlib.metadata.PackageNotFoundError: asset_base
__version__ = "1.0.0"
logging.info("Version-%s" % __version__)
