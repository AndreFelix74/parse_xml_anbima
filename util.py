#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 17:34:44 2025

@author: andrefelix
"""


import os
import inspect
import pandas as pd
from configparser import ConfigParser


def load_config(config_file):
    """
    Load and parse a configuration file.

    Args:
        config_file (str): Path to the configuration file.

    Returns:
        ConfigParser: Parsed configuration object.
    """
    config = ConfigParser()
    config.read(config_file)

    return config


def format_path(str_path):
    """
    Format a given path to ensure it starts with a proper prefix and ends with a slash.

    Args:
        str_path (str): The input file path.

    Returns:
        str: Formatted path.
    """
    if not str_path.startswith("/") and not str_path.startswith("."):
        str_path = os.path.join("..", "data", str_path)

    if not str_path.endswith("/"):
        str_path += "/"

    return str_path


def validate_required_columns(df: pd.DataFrame, required_columns: list):
    """
    Validates that all required columns are present in the given DataFrame.
    Automatically identifies the name of the calling function to include in error messages.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        required_columns (list): A list of column names that must be present.

    Raises:
        ValueError: If one or more required columns are missing.
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        caller_name = inspect.stack()[1].function
        raise ValueError(f"[{caller_name}] Missing required columns: {', '.join(missing_columns)}")

