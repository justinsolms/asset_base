#!/usr/bin/env python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

"""Utility functions and classes that can't be grouped in a specific module.

Copyright (C) 2015 Justin Solms <justinsolms@gmail.com>.
This file is part of the asset_base module.
The asset_base module can not be modified, copied and/or
distributed without the express permission of Justin Solms.

"""
import datetime

import pandas as pd


# datetime to date-string converter
def date_to_str(df):
    """Convert Timestamp objects to test date-strings."""
    df.replace({pd.NaT: None}, inplace=True)
    # All dates to date strings
    for index, row in df.iterrows():
        for column, item in row.items():
            if (
                isinstance(item, pd.Timestamp)
                or isinstance(item, datetime.date)
                or isinstance(item, datetime.datetime)
            ):
                # Not sure why I must do this 0!?! Else it does not work
                df.loc[index, column] = 0
                # Convert
                try:
                    df.loc[index, column] = item.strftime("%Y-%m-%d")
                except ValueError:
                    df.loc[index, column] = None
