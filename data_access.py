#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 17:39:16 2025

@author: andrefelix
"""


import json
import os


DIR_SYS_DATA = './sys_data/'


def _create_sys_data():
    """
    Checks if the DIR_SYS_DATA directory exists and creates it if necessary.
    """
    if not os.path.exists(DIR_SYS_DATA):
        os.makedirs(DIR_SYS_DATA)


def _load_json_file(file_name):
    """
    Loads a JSON file from the DIR_SYS_DATA directory.

    Args:
        file_name (str): The name of the file (without the .json extension).

    Returns:
        dict: The contents of the JSON file.

    Raises:
        FileNotFoundError: If the file does not exist in the DIR_SYS_DATA directory.
    """
    file_path = os.path.join(DIR_SYS_DATA, f"{file_name}.json")

    with open(file_path, "r") as file:
        return json.load(file)


def _save_json_file(file_name, values):
    """
    Saves data to a JSON file in the DIR_SYS_DATA directory.
    Ensures the DIR_SYS_DATA directory exists before saving.

    Args:
        file_name (str): The name of the file (without the .json extension).
        values (dict): The data to be saved in the JSON file.
    """
    file_path = os.path.join(DIR_SYS_DATA, f"{file_name}.json")
    with open(file_path, "w") as file:
        json.dump(values, file, indent=4)


def read(table_name):
    """
    Reads a table (JSON file) from the DIR_SYS_DATA directory.

    Args:
        table_name (str): The name of the table (file) to read.

    Returns:
        dict: The contents of the JSON file.
    """
    return _load_json_file(table_name)


def create(table_name, values):
    """
    Creates or overwrites a table (JSON file) in the DIR_SYS_DATA directory.

    Args:
        table_name (str): The name of the table (file) to create.
        values (dict): The data to be written to the JSON file.
    """
    _save_json_file(table_name, values)


# Initialization: Create the DIR_SYS_DATA directory when the module is loaded
_create_sys_data()
