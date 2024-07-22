#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""The ``asset_base`` package initialization.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the fundmanage module.
The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
import logging
import logging.config
import yaml
import os
import pkg_resources

# Package absolute root path
_ROOT = os.path.abspath(os.path.dirname(__file__))

# Data file parent path
_DATA = "data"

# Config path
_CONFIG = "config"

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
    return pkg_resources.resource_filename(_DATA, sub_path)

def get_config_file(sub_path):
    """Package path schema for configuration files.

    Parameters
    ----------
    sub_path: str
        Mandatory branch or child path.
    """
    return pkg_resources.resource_filename(_CONFIG, sub_path)

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
        path = pkg_resources.resource_filename(__name__, os.path.join(_VAR_TEST, sub_path))
    else:
        path = pkg_resources.resource_filename(__name__, os.path.join(_VAR, sub_path))
    return path

def get_package_version(package_name):
    try:
        version = pkg_resources.get_distribution(package_name).version
        return version
    except pkg_resources.DistributionNotFound:
        return "Package not found"

# Working folders - create them if they don't exist.
config_path = get_config_file("config.yaml")
with open(config_path, "r") as stream:
    config = yaml.full_load(stream)
    for directory in config["directories"]["working"].values():
        directory = os.path.expanduser(directory)
        if not os.path.exists(directory):
            os.makedirs(directory)

# Set up logging
log_config_path = get_config_file("log_config.yaml")
with open(os.path.join(log_config_path), "r") as stream:
    log_config = yaml.full_load(stream)
    logging.config.dictConfig(log_config)

# Record the current version and log it.
__version__ = get_package_version("asset_base")
logging.info("This is fundmanage version %s" % __version__)
