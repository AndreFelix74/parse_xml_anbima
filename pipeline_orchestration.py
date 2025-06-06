#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 30 11:33:22 2025

@author: andrefelix
"""


import os
import re
import multiprocessing
from collections import defaultdict
import networkx as nx
from logger import log_timing, RUN_ID

import auxiliary_loaders as aux_loader
import parse_xml_anbima as parser
import clean_and_prepare_raw_data as cleaner
import integrity_checks as checker
import carteiras_operations as crt
import enrich_and_classify_data as enricher
import compute_metrics as metrics
import investment_tree as tree
import util as utl
import data_access as dta
from file_handler import save_df


def setup_folders(paths):
    """
    Create directories if they do not already exist.

    Args:
        paths (list): List of directory paths to create.
    """
    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)


def save_intermediate(dtfrm, filename, config, log):
    """
    Saves an intermediate DataFrame to a unique RUN_ID subfolder.

    Parameters
    ----------
    dtfrm : pandas.DataFrame
        DataFrame to be saved.
    name : str
        Base filename (without extension).
    config : configparser.ConfigParser
        Config object with [Debug] and [Paths] sections.
    run_id : str
        Unique identifier for this pipeline execution.

    Returns
    -------
    str
        Full path of saved file.

    Raises
    ------
    KeyError
        If required configuration keys or sections are missing.
    ValueError
        If file writing is disabled by configuration.
    """
    run_folder = os.path.join(config['output_path'], RUN_ID)
    os.makedirs(run_folder, exist_ok=True)

    full_path = os.path.join(run_folder, filename)
    save_df(dtfrm, full_path, config['file_format'])

    log.info('intermediate_files_saved', arquivo=f"{full_path}.{config['file_format']}")


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


def parse_files(intermediate_cfg, xml_source_path, processes, daily_keys):
    with log_timing('parse', 'load_xml_files') as log:
        all_xml_files = find_all_files(xml_source_path, '.xml')
        xml_files_to_process, xml_discarted = select_latest_xml_by_cnpj_and_date(all_xml_files)

        log.info(
            'arquivos_xml_encontrados',
            total=len(all_xml_files),
            processados=len(xml_files_to_process),
            descartados=len(xml_discarted),
            dados=xml_discarted
        )

    with log_timing('parse', 'paser_xml_content') as log:
        with multiprocessing.Pool(processes=processes) as pool:
            parsed_content = pool.map(parser.parse_file, xml_files_to_process)

    with log_timing('parse', 'convert_to_pandas') as log:
        funds_list, portfolios_list = parser.split_funds_and_portfolios(parsed_content)
        funds = convert_entity_to_dataframe(funds_list, 'fundos', daily_keys)
        portfolios = convert_entity_to_dataframe(portfolios_list, 'carteiras', daily_keys)

    if intermediate_cfg['save']:
        with log_timing('parse', 'save_parsed_raw_data') as log:
            save_intermediate(funds, 'fundos-raw', intermediate_cfg, log)
            save_intermediate(portfolios, 'carteiras-raw', intermediate_cfg, log)

    return [funds, portfolios]


def clean_and_prepare_raw(intermediate_cfg, funds, portfolios, types_to_exclude,
                          types_series, harmonization_rules):
    funds_dtypes = dta.read('fundos_metadata')
    port_dtypes = dta.read('carteiras_metadata')

    funds = cleaner.clean_data(funds, funds_dtypes, types_to_exclude,
                               types_series, harmonization_rules)
    portfolios = cleaner.clean_data(portfolios, port_dtypes, types_to_exclude,
                                    types_series, harmonization_rules)

    if intermediate_cfg['save']:
        with log_timing('clean', 'save_cleaned_data') as log:
            save_intermediate(funds, 'fundos-cleaned', intermediate_cfg, log)
            save_intermediate(portfolios, 'carteiras-cleaned', intermediate_cfg, log)

    return [funds, portfolios]


def check_values_integrity(intermediate_cfg, entity, entity_name, invested, group_keys):
    investor_holdings_cols = ['cnpjfundo', 'qtdisponivel', 'dtposicao', 'isin',
                              'NOME_ATIVO', 'puposicao']

    with log_timing('check', f"check_puposicao_consistency_{entity_name}") as log:
        investor_holdings = entity[entity['cnpjfundo'].notnull()][investor_holdings_cols].copy()
        divergent_puposicao = checker.check_puposicao(investor_holdings, invested)

        if not divergent_puposicao.empty:
            log.warn('check', dados=divergent_puposicao.to_json())
            save_intermediate(divergent_puposicao,
                              f"{entity_name}_puposicao_divergente",
                              intermediate_cfg, log)

    with log_timing('check', f"check_pl_consistency_{entity_name}") as log:
        divergent_pl = checker.check_composition_consistency(entity, group_keys)

        if not divergent_pl.empty:
            log.warn('check', dados=divergent_pl.to_json())
            save_intermediate(divergent_pl, f"{entity_name}_pl_divergente",
                              intermediate_cfg, log)


def explode_partplanprev(intermediate_cfg, portfolios):
    with log_timing('enrich', 'explode_partplanprev') as log:
        allocated_partplanprev = crt.explode_partplanprev_and_allocate(portfolios)
        if allocated_partplanprev is None:
            return portfolios

    portfolios = crt.integrate_allocated_partplanprev(portfolios, allocated_partplanprev)

    if intermediate_cfg['save']:
        with log_timing('enrich', 'save_exploded_partplanprev') as log:
            save_intermediate(portfolios, 'carterias-exploded', intermediate_cfg, log)

    mask = portfolios['tipo'] == 'partplanprev'
    mask |= portfolios['flag_rateio'] == 1
    return portfolios[~mask]


def enrich(intermediate_cfg, funds, portfolios, types_series, data_aux_path,
           new_tipo_rules, gestor_name_stopwords, name_standardization_rules):

    with log_timing('enrich', 'load_aux_data') as log:
        aux_data = aux_loader.load_enrich_auxiliary_data(data_aux_path)

    with log_timing('enrich', 'merge_aux_data') as log:
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

    with log_timing('enrich', 'enrich_and_classify') as log:
        alerts = enricher.enrich_and_classify(portfolios, types_series,
                                              name_standardization_rules,
                                              new_tipo_rules, gestor_name_stopwords)
        if alerts:
            log.warning(f"Classification alerts for portfolios: {alerts}")

        alerts = enricher.enrich_and_classify(funds, types_series,
                                              name_standardization_rules,
                                              new_tipo_rules, gestor_name_stopwords)
        if alerts:
            log.warning(f"Classification alerts for funds: {alerts}")


    if intermediate_cfg['save']:
        with log_timing('enrich', 'save_enriched_data') as log:
            save_intermediate(funds, 'fundos-enriched', intermediate_cfg, log)
            save_intermediate(portfolios, 'carteiras-enriched', intermediate_cfg, log)

    return [funds, portfolios]


def compute_metrics(funds, portfolios, types_series):
    metrics.compute(funds, funds, types_series, ['cnpj'])

    group_keys_port = ['cnpjcpf', 'codcart', 'dtposicao', 'nome', 'cnpb']
    metrics.compute(portfolios, funds, types_series, group_keys_port)


def validate_fund_graph_is_acyclic(funds):
    """
    Validates that the fund-to-fund relationships form a Directed Acyclic Graph (DAG).
    Raises an exception if any cycles are found in the investment structure.

    Args:
        funds (pd.DataFrame): DataFrame containing at least 'cnpj' (invested fund) and
                              'cnpjfundo' (investor fund) columns.

    Raises:
        ValueError: If a cycle is detected in the graph of fund relationships.
    """
    edges = (
        funds[['cnpjfundo', 'cnpj']]
        .dropna()
        .drop_duplicates()
        .values
        .tolist()
    )
    graph = nx.DiGraph()
    graph.add_edges_from(edges)

    try:
        nx.algorithms.dag.topological_sort(graph)
    except nx.NetworkXUnfeasible as excpt:
        cycle = nx.find_cycle(graph, orientation='original')
        raise ValueError(f"Cycle detected in fund relationships: {cycle}") from excpt


def build_horizontal_tree(funds, portfolios, data_aux_path):
    with log_timing('tree', 'build_tree'):
        governance_struct = aux_loader.load_governance_struct(data_aux_path)
        governance_struct = governance_struct[governance_struct['KEY_VEICULO'].notna()]

        tree_horzt = tree.build_tree(funds, portfolios, governance_struct)
        tree.enrich_tree(tree_horzt, governance_struct)
        return tree_horzt


def load_config():
    config = utl.load_config('config.ini')

    xml_source_path = config['Paths']['xml_source_path']
    xml_source_path = f"{os.path.dirname(utl.format_path(xml_source_path))}/"

    xlsx_destination_path = config['Paths']['xlsx_destination_path']
    xlsx_destination_path = f"{os.path.dirname(utl.format_path(xlsx_destination_path))}/"

    data_aux_path = config['Paths']['data_aux_path']
    data_aux_path = f"{os.path.dirname(utl.format_path(data_aux_path))}/"

    if not config.has_section('Debug'):
        raise KeyError('Missing [Debug] section in config.ini')

    if not config.has_section('Paths'):
        raise KeyError('Missing [Paths] section in config.ini')

    intermediate_cfg = {
        'save': config['Debug'].get('write_intermediate_files').lower() == 'yes',
        'output_path': config['Debug'].get('intermediate_output_path'),
        'file_format': config['Paths'].get('destination_file_extension')
    }

    return [xml_source_path, xlsx_destination_path, data_aux_path, intermediate_cfg]


def run_pipeline():
    xml_source_path, xlsx_destination_path, data_aux_path, intermediate_cfg = load_config()

    setup_folders([xlsx_destination_path])

    header_daily_values = dta.read('header_daily_values')
    daily_keys = header_daily_values.keys()
    types_series = [key for key, value in header_daily_values.items() if value.get('serie', False)]

    types_to_exclude = dta.read('types_to_exclude')
    harmonization_rules = dta.read('harmonization_values_rules')

    processes = min(8, multiprocessing.cpu_count())

    funds, portfolios = parse_files(intermediate_cfg, xml_source_path,
                                    processes, daily_keys)

    funds, portfolios = clean_and_prepare_raw(intermediate_cfg, funds, portfolios,
                                              types_to_exclude, types_series,
                                              harmonization_rules)

    name_standardization_rules = dta.read('name_standardization_rules')
    new_tipo_rules = dta.read('enrich_de_para_tipos')
    gestor_name_stopwords = dta.read('gestor_name_stopwords')

    portfolios = explode_partplanprev(intermediate_cfg, portfolios)

    funds, portfolios = enrich(intermediate_cfg, funds, portfolios, types_series,
                               data_aux_path, new_tipo_rules, gestor_name_stopwords,
                               name_standardization_rules)

    check_values_integrity(intermediate_cfg, funds, 'fundos', funds, ['cnpj'])
    check_values_integrity(intermediate_cfg, portfolios, 'carteiras', funds, ['cnpjcpf'])

    compute_metrics(funds, portfolios, types_series)

    validate_fund_graph_is_acyclic(funds)

    tree = build_horizontal_tree(funds, portfolios, data_aux_path)

    with log_timing('finish', 'save_final_files'):
        save_df(funds, f"{xlsx_destination_path}fundos", 'xlsx')
        save_df(portfolios, f"{xlsx_destination_path}carteiras", 'xlsx')
        save_df(tree, f"{xlsx_destination_path}arvore_carteiras", 'xlsx')


if __name__ == "__main__":
    with log_timing('full', 'all_process'):
        run_pipeline()
