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

# Data path
_DATA = "data"

# Config path
_CONFIG = "config"

# Tests path
_TESTS = "tests"

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
    # Get the directory of the current file (__init__.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the full path to the config folder
    data_dir = os.path.join(current_dir, '..', '..', _DATA)
    # Construct the full path to the configuration file
    data_path = os.path.join(data_dir, sub_path)

    return os.path.abspath(data_path)

def get_config_path(sub_path):
    """Package path schema for configuration files.

    Parameters
    ----------
    sub_path: str
        Mandatory branch or child path.
    """
    # Get the directory of the current file (__init__.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the full path to the config folder
    config_dir = os.path.join(current_dir, '..', '..', _CONFIG)
    # Construct the full path to the configuration file
    config_path = os.path.join(config_dir, sub_path)

    return os.path.abspath(config_path)

def get_tests_path(sub_path):
    """Package path schema for test files.

    Parameters
    ----------
    sub_path: str
        Mandatory branch or child path.
    """
    # Get the directory of the current file (__init__.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the full path to the config folder
    tests_dir = os.path.join(current_dir, '..', '..', _TESTS)
    # Construct the full path to the configuration file
    tests_path = os.path.join(tests_dir, sub_path)

    return os.path.abspath(tests_path)

def get_var_path(sub_path, testing=False):
    """Package path schema for variable data such as logs and databases.

    Will put the var folder in the package folder

    Parameters
    ----------
    sub_path: str
        Mandatory branch or child path.
    testing : bool
        If `True` then the returned path string contains `/var_test/` instead of
        the default `/var/`.
    """
    # Get the directory of the current file (__init__.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the full path to the config folder
    if testing:
        var_dir = os.path.join(current_dir, '..', '..', _VAR_TEST)
    else:
        var_dir = os.path.join(current_dir, '..', '..', _VAR)
    # Construct the full path to the configuration file
    var_path = os.path.join(var_dir, sub_path)

    return os.path.abspath(var_path)

def get_package_version(package_name):
    try:
        return pkg_resources.get_distribution(package_name).version
    except pkg_resources.DistributionNotFound:
        return "Package not found"

def get_project_name(package_name):
    try:
        return pkg_resources.get_distribution(package_name).project_name
    except pkg_resources.DistributionNotFound:
        return "Package not found"

# Working folders - create them if they don't exist.
config_path = get_config_path("config.yaml")
with open(config_path, "r") as stream:
    config = yaml.full_load(stream)
    for directory in config["directories"]["working"].values():
        directory = os.path.expanduser(directory)
        if not os.path.exists(directory):
            os.makedirs(directory)

# Set up logging
log_config_path = get_config_path("log_config.yaml")
with open(os.path.join(log_config_path), "r") as stream:
    log_config = yaml.full_load(stream)
    logging.config.dictConfig(log_config)

# Record the current version and log it.
__version__ = get_package_version("asset_base")
logging.info("This is `asset_base` version %s" % __version__)
