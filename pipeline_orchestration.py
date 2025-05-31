#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 30 11:33:22 2025

@author: andrefelix
"""


import os
import re
import time
import multiprocessing
from collections import defaultdict

import auxiliary_loaders as aux_loader
import parse_xml_anbima as parser
import clean_and_prepare_raw_data as cleaner
import carteiras_operations as crt
import enrich_and_classify_data as enricher
import util as utl
import data_access as dta


def setup_folders(paths):
    """
    Create directories if they do not already exist.

    Args:
        paths (list): List of directory paths to create.
    """
    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)


def find_all_files(files_path, file_ext):
    """
    Recursively finds all files with the given extension and returns metadata.

    Args:
        files_path (str): Root directory.
        file_ext (str): File extension (e.g., '.xml').

    Returns:
        dict: {
            full_path (str): {
                'filename': str,
                'mtime': float
            }
        }
    """
    return {
        os.path.join(root, file): {
            'filename': file,
            'mtime': os.path.getmtime(os.path.join(root, file))
        }
        for root, _, files in os.walk(files_path)
        for file in files
        if file.lower().endswith(file_ext.lower())
    }


def select_latest_xml_by_cnpj_and_date(files_info):
    """
    Given a dict of {file_path: {'filename': str, 'mtime': float}}, return the most recent
    file per unique FD+CNPJ key, and list of discarded (older) duplicates.

    The key is extracted using the first pattern that ends in _YYYYMMDD.

    Args:
        files_info (dict): Mapping of full file path to metadata.

    Returns:
        tuple:
            - latest_files (list of str): Full paths to the latest XMLs.
            - discarded_files (list of str): Full paths of older duplicates.
    """
    file_date_pattern = re.compile(r'^.+?_\d{8}(?!\d)')
    grouped = defaultdict(list)

    for path, meta in files_info.items():
        key = file_date_pattern.search(meta['filename'])
        key = key.group(0) if key else meta['filename']
        grouped[key].append((path, meta['mtime']))

    latest_files = []
    discarded_files = []

    for key, group in grouped.items():
        group.sort(key=lambda x: x[1], reverse=True)
        latest_files.append(group[0][0])
        discarded_files.extend(path for path, _ in group[1:])

    return latest_files, discarded_files


def convert_entity_to_dataframe(entity_data, entity_name, daily_keys):
    """
    Converte a lista de dados de uma entidade (fundos ou carteiras) em DataFrame.
    Registra os dtypes no repositório de metadados.

    Args:
        entity_data (list): Lista de dicionários extraídos dos XMLs.
        entity_name (str): Nome da entidade ('fundos' ou 'carteiras').
        daily_keys (iterable): Chaves diárias para separação do cabeçalho.

    Returns:
        pd.DataFrame: DataFrame convertido.
    """
    utl.log_message(f"Início conversão dos dados de {entity_name} para dataframe")
    non_propagated_header_keys = ['isin']
    dataframe = parser.convert_to_dataframe(entity_data, daily_keys, non_propagated_header_keys)

    dtypes_dict = dataframe.dtypes.apply(lambda x: x.name).to_dict()
    dta.create_if_not_exists(f"{entity_name}_metadata", dtypes_dict)

    return dataframe


def merge_aux_data(cleaned, dcadplano, aux_asset, cad_fi_cvm, col_join_cad_fi_cvm):
    """
    Combina os dados principais (fundos ou carteiras) com tabelas auxiliares:
    - ativos (numeraca + emissor)
    - dCadPlano
    - CVM

    Args:
        cleaned (pd.DataFrame): DataFrame base a ser enriquecido.
        dcadplano (pd.DataFrame): Tabela dCadPlano.
        aux_asset (pd.DataFrame): Tabela de ativos.
        cad_fi_cvm (pd.DataFrame): Base da CVM com prefixo 'dCadFI_CVM.'.
        col_join_cad_fi_cvm (str): Coluna base para o join com a CVM.

    Returns:
        pd.DataFrame enriquecido.
    """
    cleaned = cleaned.merge(
        aux_asset,
        left_on='isin',
        right_on='fNUMERACA.COD_ISIN',
        how='left'
    )

    if 'cnpb' in cleaned.columns:
        cleaned = cleaned.merge(
            dcadplano.add_prefix('dCadPlano.'),
            left_on='cnpb',
            right_on='dCadPlano.CNPB',
            how='left'
        )

    return cleaned.merge(
        cad_fi_cvm,
        left_on=col_join_cad_fi_cvm,
        right_on='dCadFI_CVM.CNPJ_FUNDO',
        how='left'
    )


def run_pipeline():
    config = utl.load_config('config.ini')

    xml_source_path = config['Paths']['xml_source_path']
    xml_source_path = f"{os.path.dirname(utl.format_path(xml_source_path))}/"

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    setup_folders([xml_source_path, xlsx_destination_path])

    data_aux_path = config['Paths']['data_aux_path']
    data_aux_path = f"{os.path.dirname(utl.format_path(data_aux_path))}/"

    header_daily_values = dta.read('header_daily_values')
    daily_keys = header_daily_values.keys()
    tipos_serie = [key for key, value in header_daily_values.items() if value.get('serie', False)]

    utl.log_message(f"Início leitura dos arquivos XML na pasta {xml_source_path}")
    all_xml_files = find_all_files(xml_source_path, '.xml')
    xml_files_to_process, xml_discarted = select_latest_xml_by_cnpj_and_date(all_xml_files)

    utl.log_message(f"{len(xml_files_to_process)} arquivos encontrados")
    time_start = time.time()
    processes = min(8, multiprocessing.cpu_count())
    with multiprocessing.Pool(processes=processes) as pool:
        parsed_content = pool.map(parser.parse_file, xml_files_to_process)
    utl.print_elapsed_time(f"parse {len(xml_files_to_process)} xml files", time_start)
    utl.log_message(f"{len(xml_files_to_process)} arquivos processados")
    funds_list, portfolios_list = parser.split_funds_and_portfolios(parsed_content)

    funds = convert_entity_to_dataframe(funds_list, 'fundos', daily_keys)
    portfolios = convert_entity_to_dataframe(portfolios_list, 'carteiras', daily_keys)

    types_to_exclude = dta.read('types_to_exclude')
    types_series = [key for key, value in header_daily_values.items() if value.get('serie', True)]
    harmonization_rules = dta.read('harmonization_values_rules')

    funds_dtypes = dta.read('fundos_metadata')
    port_dtypes = dta.read('carteiras_metadata')

    funds = cleaner.clean_data(funds, funds_dtypes, types_to_exclude, types_series, harmonization_rules)
    portfolios = cleaner.clean_data(portfolios, port_dtypes, types_to_exclude, types_series, harmonization_rules)

    allocated_partplanprev = crt.explode_partplanprev_and_allocate(portfolios, tipos_serie)
    portfolios = crt.integrate_allocated_partplanprev(portfolios, allocated_partplanprev)

    aux_data = aux_loader.load_all_auxiliary_data(data_aux_path)

    portfolios = merge_aux_data(
        portfolios,
        aux_data['dcadplano'],
        aux_data['assets'],
        aux_data['cad_fi_cvm'],
        'fEMISSOR.CNPJ_EMISSOR'
    )

    funds = merge_aux_data(
        funds,
        aux_data['dcadplano'],
        aux_data['assets'],
        aux_data['cad_fi_cvm'],
        'cnpj'
    )

    name_standardization_rules = dta.read('name_standardization_rules')
    new_tipo_rules = dta.read('enrich_de_para_tipos')
    gestor_name_stopwords = dta.read('gestor_name_stopwords')

    enricher.enrich_and_classify(portfolios, tipos_serie, name_standardization_rules,
                                 new_tipo_rules, gestor_name_stopwords)

    enricher.enrich_and_classify(funds, tipos_serie, name_standardization_rules,
                                 new_tipo_rules, gestor_name_stopwords)
