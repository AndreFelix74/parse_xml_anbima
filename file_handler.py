#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 21 08:52:18 2025

@author: andrefelix
"""


import locale
from io import BytesIO, TextIOWrapper
from pathlib import Path
import pandas as pd


def get_csv_separators():
    """
    Determines the appropriate field and decimal separators based on the system locale.

    Returns
    -------
    tuple
        A tuple (field_sep, decimal_sep) where:
        - field_sep: str, separator for fields in CSV (',' or ';')
        - decimal_sep: str, decimal mark ('.' or ',')
    """
    conv = locale.localeconv()
    decimal_sep = conv['decimal_point']
    field_sep = ';' if decimal_sep == ',' else ','
    return field_sep, decimal_sep


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
        field_sep, decimal_sep = get_csv_separators()
        return pd.read_csv(full_path, dtype=dtype, sep=field_sep,
                           decimal=decimal_sep, encoding='utf-8')

    if file_format == 'xlsx':
        return pd.read_excel(full_path, dtype=dtype)

    raise ValueError(f"Unsupported file format: {file_format}")


def save_df(dtfrm, file_path, file_format):
    """
    Save a pandas DataFrame to disk in the specified format.

    Parameters
    ----------
    dtfrm : pandas.DataFrame
        The DataFrame to be saved.
    file_path : str
        Base path (without extension) where the file will be written.
    file_format : str
        File format to save: 'xlsx' or 'csv'.

    Notes
    -----
    For CSV files, the DataFrame is first serialized into an in-memory
    `BytesIO` buffer wrapped by a `TextIOWrapper`. This ensures that:

    * The DataFrame is fully serialized in memory before any disk I/O.
    * The encoding is applied incrementally during serialization.
    * The final dump to disk is a single `write_bytes` call.

    This approach reduces disk write latency because the operating
    system performs a single large sequential write, rather than many
    small writes, and avoids holding both a large Python string and its
    encoded byte representation in memory at the same time.

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
        field_sep, decimal_sep = get_csv_separators()
        encoding='utf-8'
        raw = BytesIO()
        txt = TextIOWrapper(raw, encoding=encoding, newline='')
        dtfrm.to_csv(
            txt,
            index=False,
            sep=field_sep,
            decimal=decimal_sep,
            lineterminator='\n',
            float_format="%.15f",
        )
        txt.flush()
        txt.detach()
        Path(full_path).write_bytes(raw.getvalue())
    elif file_format == 'xlsx':
        dtfrm.to_excel(full_path, index=False)
    elif file_format == 'parquet':
        dtfrm.to_parquet(full_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
