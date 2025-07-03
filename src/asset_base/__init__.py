#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""The ``asset_base`` package initialization.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the asset_base module.
The asset_base module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
import logging
import logging.config
import sys
import yaml
import os
import pkg_resources

# External MyDrive data path environment variable name
_DATA_PATH = "DATA_PATH"

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

# Log, tmp an cache path under variable data path
_LOG = "log"
_TMP = "tmp"
_CACHE = "cache"

# Authentication certificates path
_CERTIFICATES = 'certificates'

# Resources path to such as art, logos, html, css, .md, .rst, .txt, content, etc.
_RESOURCES = 'resources'
# Templates under _RESOURCES for html, css, etc.
_TEMPLATES = 'templates'
# Content under _RESOURCES for .md, .rst, .txt, etc.
_CONTENT = 'content'
# Art under _RESOURCES for images, logos, etc.
_ART = 'art'

# General output path
_OUTPUT = '~/Downloads'

def get_output_path(sub_path=None):
    output_path = os.path.expanduser(_OUTPUT)
    if not os.path.exists(output_path):
        raise FileNotFoundError("Output directory not found")
    # Add the sub_path if it is not None
    if sub_path is not None:
        output_path = os.path.join(output_path, sub_path)
    return output_path

def get_certificates_path(sub_path=None):
    current_dir = os.path.dirname(__file__)
    certificates_path = os.path.join(current_dir, _CERTIFICATES)
    if not os.path.exists(certificates_path):
        raise FileNotFoundError("Certificates directory not found")
    # Add the sub_path if it is not None
    if sub_path is not None:
        certificates_path = os.path.join(certificates_path, sub_path)
    return certificates_path

def get_resources_path(sub_path=None):
    current_dir = os.path.dirname(__file__)
    resources_path = os.path.join(current_dir, _RESOURCES)
    if not os.path.exists(resources_path):
        raise FileNotFoundError("Resources directory not found")
    # Add the sub_path if it is not None
    if sub_path is not None:
        resources_path = os.path.join(resources_path, sub_path)
    return resources_path

def get_templates_path(sub_path=None):
    resources_dir = get_resources_path()
    templates_path = os.path.join(resources_dir, _TEMPLATES)
    if not os.path.exists(templates_path):
        raise FileNotFoundError("Templates directory not found")
    # Add the sub_path if it is not None
    if sub_path is not None:
        templates_path = os.path.join(templates_path, sub_path)
    return templates_path

def get_content_path(sub_path=None):
    resources_dir = get_resources_path()
    content_path = os.path.join(resources_dir, _CONTENT)
    if not os.path.exists(content_path):
        raise FileNotFoundError("Content directory not found")
    # Add the sub_path if it is not None
    if sub_path is not None:
        content_path = os.path.join(content_path, sub_path)
    return content_path

def get_art_path(sub_path=None):
    resources_dir = get_resources_path()
    art_path = os.path.join(resources_dir, _ART)
    if not os.path.exists(art_path):
        raise FileNotFoundError("Art directory not found")
    # Add the sub_path if it is not None
    if sub_path is not None:
        art_path = os.path.join(art_path, sub_path)
    return art_path

def get_data_path(sub_path=None):
    current_dir = os.path.dirname(__file__)
    resources_path = os.path.join(current_dir, _DATA)
    if not os.path.exists(resources_path):
        raise FileNotFoundError("Resources directory not found")
    # Add the sub_path if it is not None
    if sub_path is not None:
        resources_path = os.path.join(resources_path, sub_path)
    return resources_path

def get_external_data_path(sub_path=None):
    data_path = os.environ.get(_DATA_PATH)
    if data_path is None:
        raise ValueError(f"Environment variable {_DATA_PATH} not set.")
    # Add the sub_path if it is not None
    if sub_path is not None:
        data_path = os.path.join(data_path, sub_path)
    return os.path.abspath(data_path)

def get_config_path(sub_path=None):
    # Get the directory of the current file (__init__.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the full path to the config folder
    config_path = os.path.join(current_dir, _CONFIG)
    # Add the sub_path if it is not None
    if sub_path is not None:
        config_path = os.path.join(config_path, sub_path)
    return os.path.abspath(config_path)

def get_tests_path(sub_path=None):
    # Get the directory of the current file (__init__.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the full path to the config folder
    tests_path = os.path.join(current_dir, '..', '..', _TESTS)
    # Add the sub_path if it is not None
    if sub_path is not None:
        tests_path = os.path.join(tests_path, sub_path)
    return os.path.abspath(tests_path)

def get_var_path(sub_path=None, testing=False):
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
        var_path = os.path.join(current_dir, '..', '..', _VAR_TEST)
    else:
        var_path = os.path.join(current_dir, '..', '..', _VAR)
    # Create the var directory if it does not exist
    if not os.path.exists(var_path):
        os.makedirs(var_path)
    # Add the sub_path if it is not None
    if sub_path is not None:
        var_path = os.path.abspath(os.path.join(var_path, sub_path))
    return var_path

def get_log_path(sub_path=None, testing=False):
    var_dir = get_var_path(testing=testing)
    log_path = os.path.join(var_dir, _LOG)
    # Create the log directory if it does not exist
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    # Add the sub_path if it is not None
    if sub_path is not None:
        log_path = os.path.join(log_path, sub_path)
    return log_path

def get_tmp_path(sub_path=None, testing=False):
    var_dir = get_var_path(testing=testing)
    tmp_path = os.path.join(var_dir, _TMP)
    # Create the tmp directory if it does not exist
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)
    # Add the sub_path if it is not None
    if sub_path is not None:
        tmp_path = os.path.join(tmp_path, sub_path)
    return tmp_path

def get_cache_path(sub_path=None, testing=False):
    var_dir = get_var_path(testing=testing)
    cache_path = os.path.join(var_dir, _CACHE)
    # Create the cache directory if it does not exist
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
    # Add the sub_path if it is not None
    if sub_path is not None:
        cache_path = os.path.join(cache_path, sub_path)
    return cache_path

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

# Lines to prevent __init__.py from being executed more than once
if not hasattr(sys.modules[__name__], '_INITIALIZED'):
    log_config_file_path = get_config_path("log_config.yaml")
    with open(os.path.join(log_config_file_path), "r") as stream:
        log_config = yaml.full_load(stream)
        logging.config.dictConfig(log_config)
    # Record the current version and log it.
    __version__ = get_package_version("asset_base")
    # logging.info("This is `asset_base` version %s" % __version__)

    # Set the _INITIALIZED flag to True to prevent re-execution of the above
    # lines.
    setattr(sys.modules[__name__], '_INITIALIZED', True)
