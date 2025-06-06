#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 21 08:52:18 2025

@author: andrefelix
"""


import pandas as pd


def load_df(file_path, file_format, dtype=None):
    """
    Loads a data file into a pandas DataFrame using the specified format.

    Parameters
    ----------
    path : str
        Directory path where the file is located (must end with a slash or backslash).
    entity_name : str
        Base name of the entity file (without suffix like '_raw' or extension).
    file_format : str
        File format to read ('xlsx' or 'csv').
    dtype : dict or None, optional
        Dictionary specifying column data types to enforce during reading.

    Returns
    -------
    pandas.DataFrame
        Loaded DataFrame with the specified column types.

    Raises
    ------
    ValueError
        If the file format is not supported.
    """
    full_path = f"{file_path}.{file_format}"

    if file_format == 'csv':
        return pd.read_csv(full_path, dtype=dtype, sep=';', encoding='utf-8')

    if file_format == 'xlsx':
        return pd.read_excel(full_path, dtype=dtype)

    raise ValueError(f"Unsupported file format: {file_format}")


def save_df(dtfrm, file_path, file_format):
    """
    Saves a pandas DataFrame to a file using the specified format.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to be saved.
    path : str
        Directory path where the file will be written (must end with a slash or backslash).
    entity_name : str
        Base name of the entity file (without suffix or extension).
    file_format : str
        File format to save ('xlsx' or 'csv').

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the file format is not supported.
    """
    full_path = f"{file_path}.{file_format}"

    if file_format == 'csv':
        dtfrm.to_csv(full_path, index=False, sep=';', decimal=",", encoding='utf-8')
    elif file_format == 'xlsx':
        dtfrm.to_excel(full_path, index=False)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
