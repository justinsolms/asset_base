#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Package initialization.

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


def get_data_path(sub_path):
    """Package path schema.

    Parameters
    ----------
    sub_path: str
        Mandatory branch or child path.
    file_name : str, optional
        Return the name of the file to be found at the full path. If none
        is provided then only the folder path is returned.
    """
    return os.path.join(_ROOT, _DATA, sub_path)

