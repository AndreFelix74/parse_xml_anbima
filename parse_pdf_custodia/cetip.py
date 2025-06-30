#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 30 11:55:54 2025

@author: andrefelix
"""


import re
from .base import parse_file_base


def get_header(page_text):
    """
    Extracts participant name and code from the CETIP custody report.

    Args:
        page_text (str): Text content of the PDF page.

    Returns:
        list[Optional[str]]: List with [code, participant name]. None values if
            not found.
    """
    rgx_particpnt = r'([A-Z0-9 \-]+)\n([\d\.\-]+)\n'

    part_match= re.search(rgx_particpnt, page_text)

    if part_match:
        return [part_match.group(2).strip(), part_match.group(1).strip()]

    return [None, None]


def parse_file(file_name):
    """
    Parses CETIP custody PDF and returns structured, normalized data rows.

    Args:
        file_name (str): Path to the CETIP custody PDF file.

    Returns:
        list[list[Optional[str]]]: Parsed and cleaned data rows with participant
            info and field values.
    """
    rgx_fields = {
        'codigo': r'([0-9A-Z]+\s*?\n)',
        'sigla': r'([A-Z]+\s*?\n)',
        'data inicio': r'(\d{2}/\d{2}/\d{4}\s*?\n)?',
        'data vencimento': r'(\d{2}/\d{2}/\d{4}\s*?\n)?',
        'data ref pu': r'(\d{2}/\d{2}/\d{4}\s*?\n)',
        'pu': r'([\d\.,]+\n)',
        'quantidade': r'([\d\.,]+\n)',
        'financeiro': r'([\d\.,]+\n)',
        'tipo': r'([A-Z ]+\n)',
    }

    return parse_file_base(file_name, rgx_fields, get_header)
