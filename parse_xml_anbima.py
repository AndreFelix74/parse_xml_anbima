#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 26 10:54:51 2024

@author: andrefelix
"""


import xml.etree.ElementTree as ET
import re
from collections import defaultdict


def parse_decimal_value(value):
    """
    Parse and convert a monetary value from string to float, removing currency symbols.

    Args:
        value (str): Monetary value as a string.

    Returns:
        float or str: Parsed monetary value as float if convertible, otherwise the original string.
    """
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None

        if not any(c.isdigit() for c in value):
            return value

        vl_dec = value.replace('R$', '').replace('$', '').replace(' ', '')

        if re.match(r'^-?\d+?\.\d+$', vl_dec) or re.match(r'^\.\d+$', vl_dec):
            if vl_dec.startswith('.'):
                vl_dec = '0' + vl_dec
            elif vl_dec.startswith('-.'):
                vl_dec = vl_dec.replace('-.', '-0.')

            return float(vl_dec)

    return value


def parse_file(file_name):
    """
    Parse the contents of an XML file and extract its structured data.

    Args:
        file_name (str): Path to the XML file.

    Returns:
        defaultdict: Parsed data grouped by XML tags.
    """
    root = ET.parse(file_name).getroot()

    if root is None or len(root) == 0:
        raise ValueError(f"{file_name} without root node.")

    if root.find('.//header') is None:
        raise ValueError('header not found')

    return extract_node_data(root)


def extract_node_data(root):
    """
    Traverse the XML tree and extract structured data,
    including special handling for nested tags such as <compromisso> inside <titulopublico>.

    Args:
        root (Element): Root element of the parsed XML.

    Returns:
        defaultdict: Dictionary mapping each tag to a list of extracted records.
    """
    inline_hildren = {
        'titpublico': {'compromisso'},
    }

    data = defaultdict(list)

    for parent in root.findall(".//*"):
        for child in parent:
            if len(child) == 0:
                continue

            if child.tag in inline_hildren.get(parent.tag, set()):
                continue

            node_data = {}

            for subchild in child:
                if subchild.tag in inline_hildren.get(child.tag, set()):
                    for nested in subchild:
                        key = f"{subchild.tag}_{nested.tag}"
                        node_data[key] = parse_decimal_value(nested.text)
                else:
                    node_data[subchild.tag] = parse_decimal_value(subchild.text)

            data[child.tag].append(node_data)

    return data


def split_funds_and_portfolios(xml_content):
    """
    Process parsed XML data and split it into fund and portfolio data.

    Args:
        xml_content (list): List of parsed XML file contents.

    Returns:
        list: Two lists, one for funds and one for portfolios.
    """
    funds = []
    portfolios = []

    for file_data in xml_content:
        header = []
        posicao = defaultdict(list)

        for key, value in file_data.items():
            if key == 'header':
                header_data = file_data['header'][0]
                header.append(header_data)
                continue
            for entry in value:
                posicao[key].append(entry)

        if 'header' in file_data:
            joined_data = {'header': header_data, 'posicao': posicao}
            if header_data.get('cnpjcpf', None) is None:
                funds.append(joined_data)
            else:
                portfolios.append(joined_data)
        else:
            raise ValueError('header not found')

    return [funds, portfolios]


def split_header(header, daily_keys):
    """
    Split the header into static fund information and daily financial information.

    Args:
        header (dict): Header dictionary with various attributes.

    Returns:
        tuple: Two dictionaries, one for fund info and one for daily info.
    """
    fund_info = {}
    daily_info = {}

    for key, value in header.items():
        if key in daily_keys:
            daily_info[key] = value
        else:
            fund_info[key] = value

    return fund_info, daily_info


def convert_to_dataframe(data_list, daily_keys, non_propagated_header_keys):
    """
    Convert structured fund and portfolio data into a pandas DataFrame.

    Args:
        data_list (list): List of data dictionaries.

    Returns:
        pandas.DataFrame: DataFrame containing the combined data.
    """
    all_rows = []

    for joined_data in data_list:
        header_fixed_info, header_daily_values = split_header(joined_data['header'], daily_keys)

        for key in non_propagated_header_keys:
            header_fixed_info.pop(key, None)

        for daily_key, value in header_daily_values.items():
            row = {**header_fixed_info, 'tipo': daily_key, 'valor': value}
            all_rows.append(row)

        posicao = joined_data['posicao']

        for tp_atv, entries in posicao.items():
            for entry in entries:
                row = {**header_fixed_info, **entry, 'tipo': tp_atv}
                all_rows.append(row)

    return all_rows
