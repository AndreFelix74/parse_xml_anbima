#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  8 17:34:44 2025

@author: andrefelix
"""


import os
import warnings
from configparser import ConfigParser
from colorama import init, Fore, Style


init(autoreset=True)


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


def log_message(msg, level='info'):
    """
    Prints a formatted message to the terminal based on the specified severity level.
    Uses color highlighting (via colorama) to visually distinguish message types.

    Parameters:
        msg (str): The message to display.
        level (str): The severity level of the message. Accepted values:
            - 'info'     : General information (cyan)
            - 'success'  : Successful operation (green)
            - 'debug'    : Debugging message (blue)
            - 'warn'     : Non-critical warning (yellow)
            - 'error'    : Critical error or failure (red)
            - Any other value will be treated as 'unknown'.

    Returns:
        None. The function outputs the message to the terminal.
    """
    level = level.lower()

    if level == 'info':
        print(Fore.CYAN + '[INFO]\n  ' + msg)
    elif level == 'success':
        print(Fore.GREEN + '[OK]\n  ' + msg)
    elif level == 'debug':
        print(Fore.BLUE + '[DEBUG]\n  ' + msg)
    elif level == 'warn' or level == 'warning':
        print(Fore.YELLOW + '[WARNING]\n  ' + msg)
    elif level == 'error':
        print(Fore.RED + '[ERROR]\n  ' + msg)
    else:
        warnings.warn('[UNKNOWN]\n  ' + msg)
