#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 26 10:54:51 2024

@author: andrefelix
"""


import xml.etree.ElementTree as ET
import time
import multiprocessing
import os
import re
from collections import defaultdict
import pandas as pd
import util as utl
import data_access as dta


COUNT_PARSE = multiprocessing.Value('i', 0)


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

        vl_dec = value.replace('R$', '').replace('$', '').replace(' ', '')

        if re.match(r'^-?\d+?\.\d+$', vl_dec) or re.match(r'^\.\d+$', vl_dec):
            if vl_dec.startswith('.'):
                vl_dec = '0' + vl_dec
            elif vl_dec.startswith('-.'):
                vl_dec = vl_dec.replace('-.', '-0.')

            return float(vl_dec)

    return value


def get_xml_files(files_path):
    """
    Retrieve and sort all XML files in a given directory and its subdirectories.

    Args:
        files_path (str): Path to the directory containing XML files.

    Returns:
        list: List of absolute paths to XML files.
    """
    lst_files = sorted(
        [
            os.path.join(root, file)
            for root, dirs, files in os.walk(files_path)
            for file in files
            if file.lower().endswith(".xml")
        ]
    )

    return lst_files


def parse_files(str_file_name):
    """
    Parse the contents of an XML file and extract its structured data.

    Args:
        str_file_name (str): Path to the XML file.

    Returns:
        defaultdict: Parsed data grouped by XML tags.
    """
    global COUNT_PARSE

    with COUNT_PARSE.get_lock():
        COUNT_PARSE.value += 1
        if COUNT_PARSE.value % 100 == 0:
            print(f"Parsing {COUNT_PARSE.value}th file")

    data = defaultdict(list)

    root = None

    try:
        root = ET.parse(str_file_name).getroot()
    except KeyboardInterrupt:
        return []
    except Exception as excpt:
        print(excpt)

    if root is None or len(root) == 0:
        print(f"{str_file_name} without root node.")
        return data

    if root.find('.//header') is None:
        print(f"{str_file_name} is missing a 'header' node.")
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
    INLINE_CHILDREN = {
        'titpublico': {'compromisso'},
    }

    data = defaultdict(list)

    for parent in root.findall(".//*"):
        for child in parent:
            if len(child) == 0:
                continue

            if child.tag in INLINE_CHILDREN.get(parent.tag, set()):
                continue

            node_data = {}

            for subchild in child:
                if subchild.tag in INLINE_CHILDREN.get(child.tag, set()):
                    for nested in subchild:
                        key = f"{subchild.tag}_{nested.tag}"
                        node_data[key] = parse_decimal_value(nested.text)
                else:
                    node_data[subchild.tag] = parse_decimal_value(subchild.text)

            data[child.tag].append(node_data)

    return data


def read_data_from_parsed_data(xml_content):
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


def split_header(header):
    """
    Split the header into static fund information and daily financial information.

    Args:
        header (dict): Header dictionary with various attributes.

    Returns:
        tuple: Two dictionaries, one for fund info and one for daily info.
    """
    header_daily_values = dta.read('header_daily_values')
    daily_keys = header_daily_values.keys()

    fund_info = {}
    daily_info = {}

    for key, value in header.items():
        if key in daily_keys:
            daily_info[key] = value
        else:
            fund_info[key] = value

    return fund_info, daily_info


def convert_to_dataframe(data_list):
    """
    Convert structured fund and portfolio data into a pandas DataFrame.

    Args:
        data_list (list): List of data dictionaries.

    Returns:
        pandas.DataFrame: DataFrame containing the combined data.
    """
    all_rows = []

    for joined_data in data_list:
        header_fixed_info, header_daily_values = split_header(joined_data['header'])

        for daily_key, value in header_daily_values.items():
            row = {**header_fixed_info, 'tipo': daily_key, 'valor': value}
            all_rows.append(row)

        posicao = joined_data['posicao']

        for tp_atv, entries in posicao.items():
            for entry in entries:
                row = {**header_fixed_info, **entry, 'tipo': tp_atv}
                all_rows.append(row)

    return pd.DataFrame(all_rows)


def print_elapsed_time(step, start_time):
    """
    Print the time elapsed for a specific processing step.

    Args:
        step (str): Description of the processing step.
        start_time (float): Start time of the process.
    """
    elapsed_time = time.time() - start_time
    print(f"{round(elapsed_time, 3)} secs to {step}")


def setup_folders(paths):
    """
    Create directories if they do not already exist.

    Args:
        paths (list): List of directory paths to create.
    """
    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)


def main():
    """
    Main function to process XML files, parse data, and export to Excel.
    """
    config = utl.load_config('config.ini')

    xml_source_path = config['Paths']['xml_source_path']
    xml_source_path = f"{os.path.dirname(utl.format_path(xml_source_path))}/"

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    setup_folders([xml_source_path, xlsx_destination_path])

    lst_files = get_xml_files(f"{xml_source_path}")

    pool = multiprocessing.Pool()
    time_start = time.time()
    xml_content = pool.map(parse_files, lst_files)
    print_elapsed_time('load xml', time_start)

    time_start = time.time()
    lst_data = read_data_from_parsed_data(xml_content)
    print_elapsed_time('parse xml', time_start)

    for idx, data in enumerate(lst_data):
        file_name = 'fundos' if idx % 2 == 0 else 'carteiras'
        dataframe = convert_to_dataframe(data)

        dtypes_dict = dataframe.dtypes.apply(lambda x: x.name).to_dict()

        dta.create(f"{file_name}_metadata", dtypes_dict)

        dataframe.to_excel(f"{xlsx_destination_path}{str(file_name)}_raw.xlsx", index=False)


if __name__ == "__main__":
    main()
