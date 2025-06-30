#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 30 17:57:20 2025

@author: andrefelix
"""


import re
import fitz


def extract_raw_rows(file_name, rgx_fields, get_header):
    """
    Extracts raw rows from a PDF file using regular expressions and a header function.

    Args:
        file_name (str): Path to the PDF file.
        rgx_fields (dict): Dictionary mapping field names to regular expression patterns.
        get_header (Callable[[str], list]): Function that extracts a list of 
        header values from the PDF text.

    Returns:
        list[list[str]]: List of raw rows, each as a list combining header
        values and extracted field values.
    """
    pdf_doc = fitz.open(file_name)
    patt_row = re.compile(''.join(rgx_fields.values()))

    raw_rows = []
    header = None

    for page in pdf_doc:
        page_text = page.get_text()

        if header is None:
            header = get_header(page_text)

        matches = patt_row.findall(page_text)
        for row_match in matches:
            raw_rows.append(header + list(row_match) + [file_name])

    return raw_rows


def parse_file_base(file_name, rgx_fields, get_header):
    """
    Parses and normalizes rows from a PDF file based on field patterns and header metadata.

    Args:
        file_name (str): Path to the PDF file.
        rgx_fields (dict): Dictionary of field names and their corresponding
            regex patterns.
        get_header (Callable[[str], list]): Function that extracts header
            metadata from PDF text.

    Returns:
        list[list[Optional[str]]]: List of cleaned and normalized rows as
            lists of strings or None.
    """
    raw_rows = extract_raw_rows(file_name, rgx_fields, get_header)
    rows = []

    for raw_row in raw_rows:
        row = []
        for raw_field in raw_row:
            if raw_field is None:
                field = None
            else:
                field = raw_field.strip().replace('.', '').replace(',', '.')
            row.append(field)
        rows.append(row)

    return rows
