#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 30 11:56:15 2025

@author: andrefelix
"""


import re
from .base import parse_file_base


def get_header(page_text):
    """
    Extracts account and custody date from the SELIC custody report.

    Args:
        page_text (str): Text content of the PDF page.

    Returns:
        list[Optional[str]]: List with [account, custody date]. None values if
            not found.
    """
    conta_match = re.search(r'\n(.+)\nConta:\n', page_text)
    posicao_match = re.search(r'\nExtrato de Custódia em (\d{2}/\d{2}/\d{4})\n', page_text)

    conta = conta_match.group(1).strip() if conta_match else None
    posicao = posicao_match.group(1).strip() if posicao_match else None

    return [conta, posicao]


def parse_file(file_name):
    """
    Parses SELIC custody PDF and returns structured, normalized data rows.

    Args:
        file_name (str): Path to the SELIC custody PDF file.

    Returns:
        list[list[Optional[str]]]: Parsed and cleaned data rows with account
            and date information.
    """
    rgx_fields = {
        'carteira c/d': r'\n([C-D]\n)',
        'carteira qtd': r'([\d\.,]+\n)',
        'a revender': r'([\d\.,]+\n)',
        'a recomprar': r'([\d\.,]+\n)',
        'isin': r'([0-9A-Z]*\n)',
        'fechamento': r'([\d\.,]+\n)',
        'abertura': r'([\d\.,]+\n)',
        'titulo venc': r'(\d{2}\/\d{2}\/\d{4}\n)',
        'titulo nome': r'([0-9A-Z\-]*\s*)',
        'titulo cod': r'(\n?[0-9A-Z\- ]*\n)',
    }

    return parse_file_base(file_name, rgx_fields, get_header)
