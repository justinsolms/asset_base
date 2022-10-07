#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""The ``asset_base`` package initialization.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the fundmanage module.
The fundmanage module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
import os

# Package absolute root path
_ROOT = os.path.abspath(os.path.dirname(__file__))

# Data file parent path
_DATA = 'data'

# Variable data path
_VAR = 'var'


def get_data_path(sub_path):
    """Package path schema for fixed data

    Parameters
    ----------
    sub_path: str
        Mandatory branch or child path.
    file_name : str, optional
        Return the name of the file to be found at the full path. If none
        is provided then only the folder path is returned.
    """
    return os.path.join(_ROOT, _DATA, sub_path)

def get_var_path(sub_path):
    """Package path schema for variable data such as logs and databases.

    Parameters
    ----------
    sub_path: str
        Mandatory branch or child path.
    file_name : str, optional
        Return the name of the file to be found at the full path. If none
        is provided then only the folder path is returned.
    """
    return os.path.join(_ROOT, _VAR, sub_path)
