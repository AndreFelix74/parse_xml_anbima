#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 30 11:33:22 2025

@author: andrefelix
"""


from datetime import datetime
import os
import re
import locale
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
import networkx as nx
import pandas as pd
from logger import log_timing, RUN_ID

import auxiliary_loaders as aux_loader
import parse_xml_anbima as parser
import clean_and_prepare_raw_data as cleaner
import integrity_checks as checker
import carteiras_operations as crt
import enrich_and_classify_data as enricher
import compute_metrics as metrics
from returns import (
    compute_plan_returns_adjustment,
    compute_returns_from_puposicao,
    validate_unique_puposicao
    )
from investment_tree import build_tree, enrich_text, enrich_values
from reporting import assign_governance_struct_keys
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


def _save_file_with_run_id(dtfrm, filename, root_path, file_fmt, log, log_msg_key):
    """
    Helper function to save a DataFrame to a RUN_ID subfolder.
    """
    run_folder = os.path.join(root_path, RUN_ID)
    os.makedirs(run_folder, exist_ok=True)

    full_path = os.path.join(run_folder, filename)
    save_df(dtfrm, full_path, file_fmt)

    log.info(log_msg_key, arquivo=f"{full_path}.{file_fmt}")


def debug_save(dtfrm, filename, config, timing_msg, timing_detail_msg):
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

    Raises
    ------
    KeyError
        If required configuration keys or sections are missing.
    ValueError
        If file writing is disabled by configuration.
    """
    if not config['save']:
        return

    with log_timing(timing_msg, timing_detail_msg) as log:
        _save_file_with_run_id(
            dtfrm,
            filename,
            config['output_path'],
            config['file_format'],
            log, 
            'intermediate_files_saved'
        )


def save_log_evidence(dtfrm, filename, config, log):
    """
    Saves a DataFrame as evidence of a validation error/warning.
    Uses RUN_ID to create a subfolder within the evidence path.
    """
    _save_file_with_run_id(
        dtfrm,
        filename,
        config['log_evidence_root'],
        config['log_evidence_file_format'],
        log,
        'evidence_saved'
    )


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


def find_all_mecsac_files(files_path):
    """
    Recursively finds all files mec_sac, and returns metadata.

    Args:
        files_path (str): Root directory.

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
        if file.startswith('_mecSAC_') and file.endswith('.xlsx')
    }


def load_mecsac(mec_source_path, processes):
    with log_timing('load', 'find_mecsac_files') as log:
        all_mecsac_files = find_all_mecsac_files(mec_source_path)

        log.info(
            'load',
            total=len(all_mecsac_files),
        )

    with log_timing('load', 'load_mecsac_content'):
        with ProcessPoolExecutor(max_workers=processes) as executor:
            dfs = list(executor.map(aux_loader.load_mecsac_file,
                                    all_mecsac_files))

        mec_sac = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    return mec_sac


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
    entity_rows = parser.flatten_data(entity_data, daily_keys, non_propagated_header_keys)

    dataframe = pd.DataFrame(entity_rows)
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


def create_numeric_fields_set(funds_dtypes, port_dtypes):
    numeric_types = ['float64', 'int64', 'float', 'int']
    return (
        {field for field, dtype in funds_dtypes.items()
        if dtype in numeric_types}
        |
        {field for field, dtype in port_dtypes.items()
        if dtype in numeric_types}
    )


def parse_files(debug_cfg, xml_source_path, processes, daily_keys, numeric_fields):
    with log_timing('parse', 'find_xml_files') as log:
        all_xml_files = find_all_files(xml_source_path, '.xml')
        xml_files_to_process, xml_discarted = select_latest_xml_by_cnpj_and_date(all_xml_files)

        log.info(
            'parse',
            total=len(all_xml_files),
            processados=len(xml_files_to_process),
            descartados=len(xml_discarted),
            dados=[{"nome_arquivo_descartado": nome} for nome in xml_discarted]
        )

    with log_timing('parse', 'parse_xml_content'):
        with multiprocessing.Pool(processes=processes) as pool:
            parsed_content = pool.starmap(parser.parse_file, [
                (file, numeric_fields)
                for file in xml_files_to_process
            ])

    with log_timing('parse', 'convert_to_pandas'):
        funds_list, portfolios_list = parser.split_funds_and_portfolios(parsed_content)
        funds = convert_entity_to_dataframe(funds_list, 'fundos', daily_keys)
        portfolios = convert_entity_to_dataframe(portfolios_list, 'carteiras', daily_keys)

    debug_save(funds, 'fundos-raw', debug_cfg, 'parse', 'debug_save_parsed_raw_data')
    debug_save(portfolios, 'carteiras-raw', debug_cfg, 'parse', 'debug_save_parsed_raw_data')

    return [funds, portfolios]


def clean_and_prepare_raw(debug_cfg, funds, portfolios, types_to_exclude,
                          types_series, harmonization_rules, funds_dtypes, port_dtypes):
    with log_timing('clean', 'clean_and_prepare'):
        funds = cleaner.clean_data(funds, funds_dtypes, types_to_exclude,
                                   types_series, harmonization_rules)

        portfolios = cleaner.clean_data(portfolios, port_dtypes, types_to_exclude,
                                        types_series, harmonization_rules)

    debug_save(funds, 'fundos-cleaned', debug_cfg, 'clean', 'debug_save_cleaned_data')
    debug_save(portfolios, 'carteiras-cleaned', debug_cfg, 'clean', 'debug_save_cleaned_data')

    return [funds, portfolios]


def check_puposicao_consistency_merge(inconsistenci_data, entity, cols_entity):
    return inconsistenci_data.merge(
        entity[cols_entity + ['isin', 'dtposicao']],
        on=['isin', 'dtposicao'],
        how='inner',
    )


def check_puposicao_consistency(debug_cfg, funds, portfolios):
    with log_timing('check', 'puposicao_consistency') as log:
        group_cols = ['isin', 'dtposicao', 'puposicao']
        funds_mask = funds['isin'].notnull() & (funds['NEW_TIPO'] != 'OVER')
        port_mask = portfolios['isin'].notnull() & (portfolios['NEW_TIPO'] != 'OVER')
        isin_data = pd.concat([
            funds[funds_mask][group_cols].drop_duplicates(),
            portfolios[port_mask][group_cols].drop_duplicates(),
            ],
            ignore_index=True).drop_duplicates()

        inconsistent_groups = (
            isin_data.groupby(['isin', 'dtposicao'])['puposicao']
            .nunique()
            .reset_index()
            .rename(columns={'puposicao': 'count_diff_puposicao'})
        )

        inconsistent_groups = inconsistent_groups[inconsistent_groups['count_diff_puposicao'] > 1]

        if not inconsistent_groups.empty:
            base_cols = ['nome', 'puposicao', 'NEW_NOME_ATIVO', 'NEW_TIPO']
            inconsistent_port = check_puposicao_consistency_merge(
                inconsistent_groups, portfolios[port_mask], ['cnpjcpf', 'codcart', 'cnpb'] + base_cols 
                )
            inconsistent_funds = check_puposicao_consistency_merge(
                inconsistent_groups, funds[funds_mask], ['cnpj'] + base_cols
                )
            all_inconsistencies = pd.concat([inconsistent_port, inconsistent_funds], ignore_index=True)

            all_inconsistencies.sort_values(['isin', 'dtposicao', 'puposicao'], inplace=True)

            log.warn(
                'check',
                message='puposicao diferente para mesmo isin e dtposicao primeiras 100 diferencas.',
                dados=all_inconsistencies[0:100].to_dict(orient='records')
            )

            save_log_evidence(all_inconsistencies, 'puposicao_divergente_mesma_data', debug_cfg, log)


def check_values_integrity(debug_cfg, entity, entity_name, invested, group_keys):
    investor_holdings_cols = ['cnpjfundo', 'qtdisponivel', 'dtposicao', 'isin',
                              'nome', 'puposicao']

    with log_timing('check', f"puposicao_vs_vlcota_{entity_name}") as log:
        investor_holdings = entity[entity['cnpjfundo'].notnull()][investor_holdings_cols].copy()
        divergent_puposicao_vlcota = checker.check_puposicao_vs_valorcota(investor_holdings, invested)

        if not divergent_puposicao_vlcota.empty:
            log.warn('check', dados=divergent_puposicao_vlcota.to_dict(orient="records"))
            save_log_evidence(divergent_puposicao_vlcota,
                              f"{entity_name}_puposicao_divergente_vlcota", debug_cfg, log)

    with log_timing('check', f"pl_consistency_{entity_name}") as log:
        divergent_pl = checker.check_composition_consistency(entity, group_keys, 0.01 / 100.0)

        if not divergent_pl.empty:
            log.warn('check', dados=divergent_pl.to_dict(orient="records"))
            save_log_evidence(divergent_pl, f"{entity_name}_pl_divergente", debug_cfg, log)


def explode_partplanprev(debug_cfg, portfolios):
    with log_timing('enrich', 'explode_partplanprev'):
        allocated_partplanprev = crt.explode_partplanprev_and_allocate(portfolios)
        if allocated_partplanprev is None:
            return portfolios

        portfolios = crt.integrate_allocated_partplanprev(portfolios, allocated_partplanprev)

    debug_save(portfolios, 'carterias-exploded', debug_cfg, 'enrich', 'debug_save_exploded_partplanprev')

    with log_timing('enrich', 'remove_partplanprev'):
        mask = portfolios['tipo'] == 'partplanprev'
        mask |= portfolios['flag_rateio'] == 1

    return portfolios[~mask]


def enrich(debug_cfg, funds, portfolios, types_series, data_aux_path, dcadplano,
           new_tipo_rules, gestor_name_stopwords, name_standardization_rules):

    with log_timing('enrich', 'load_aux_data'):
        aux_data = aux_loader.load_enrich_auxiliary_data(data_aux_path)

    with log_timing('enrich', 'merge_aux_data'):
        portfolios = merge_aux_data(
            portfolios,
            dcadplano,
            aux_data['assets'],
            aux_data['cad_fi_cvm'],
            'fEMISSOR.CNPJ_EMISSOR'
        )

        funds = merge_aux_data(
            funds,
            dcadplano,
            aux_data['assets'],
            aux_data['cad_fi_cvm'],
            'cnpjfundo'
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

    debug_save(funds, 'fundos-enriched', debug_cfg, 'enrich', 'debug_save_enriched_data')
    debug_save(portfolios, 'carteiras-enriched', debug_cfg, 'enrich', 'debug_save_enriched_data')

    return [funds, portfolios]


def compute_metrics(funds, portfolios, types_series):
    with log_timing('compute', 'metrics'):
        metrics.compute(funds, funds, types_series, ['cnpj'])

        group_keys_port = ['cnpjcpf', 'codcart', 'dtposicao', 'nome', 'cnpb']
        metrics.compute(portfolios, funds, types_series, group_keys_port)


def extract_portfolio_submassa(debug_cfg, cad_submassa, portfolios):
    with log_timing('submassa', 'extract_portfolio_submassa'):
        mask = portfolios['codcart'].isin(cad_submassa['CODCART'])

        port_submassa = portfolios.loc[mask].merge(
            cad_submassa,
            left_on='codcart',
            right_on='CODCART',
            how='left',
        )

    debug_save(port_submassa, 'submassa-carterias', debug_cfg, 'submassa', 'debug_save_portfolio_submassa')

    return [portfolios.loc[~mask], port_submassa]


def compute_composition_portfolio_submassa(debug_cfg, port_submassa):
    with log_timing('submassa', 'compute_composition_portfolio_submassa'):
        port_submassa['total_submassa_isin_cnpb'] = (
            port_submassa.groupby(['dtposicao', 'CNPB', 'isin'])['valor_calc'].transform('sum')
        )

        port_submassa['pct_submassa_isin_cnpb'] = (
            port_submassa['valor_calc'] / port_submassa['total_submassa_isin_cnpb']
        )

    debug_save(port_submassa, 'submassa-carterias-composicao', debug_cfg, 'submassa', 'debug_save_portfolio_submassa_composition')


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
    with log_timing('check', 'acyclic_graph'):
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


def assign_returns(entity, entity_key, entity_name):
    with log_timing('compute', f"returns_{entity_name}"):
        entity.sort_values(by=entity_key + ['isin', 'dtposicao'], inplace=True)
        pct = entity.groupby(entity_key + ['isin'])['puposicao'].pct_change(fill_method=None)
        entity['rentab'] = pct.round(8)

    with log_timing('compute', f"returns_{entity_name}_over"):
        mask_over = entity['NEW_TIPO'] == 'OVER'

        if mask_over.any():
            entity.loc[mask_over, 'rentab'] = (
                entity.loc[mask_over, 'compromisso_puretorno']
                / entity.loc[mask_over, 'pucompra']
            ) - 1

    return entity


def build_horizontal_tree(debug_cfg, funds, portfolios, port_submassa):
    with log_timing('tree', 'build_tree'):
        tree_horzt = build_tree(funds, portfolios)

        mask = (
            tree_horzt['cnpb'].isin(port_submassa['CNPB'].unique()) &
            tree_horzt['dtposicao'].isin(port_submassa['dtposicao'].unique())
        )

        tree_horzt_submassa = tree_horzt[mask].copy()

    debug_save(tree_horzt_submassa, 'submassa-arvore', debug_cfg, 'tree', 'debug_save_tree_submassa')

    return tree_horzt.loc[~mask], tree_horzt_submassa

#PASSAR EXPLODE SUBMASSA para tree/tree_operations como eh com carteira_operations
def explode_horizontal_tree_submassa(debug_cfg, tree_horzt_sub, port_submassa):
    with log_timing('tree', 'build_tree_submassa'):
        cols_port_submassa = ['dtposicao', 'CNPB', 'isin', 'CODCART',
                              'COD_SUBMASSA', 'SUBMASSA', 'pct_submassa_isin_cnpb']
        mask_port = (~port_submassa['isin'].isna())

        tree_horzt_sub['COD_SUBMASSA'] = None
        tree_horzt_sub['SUBMASSA'] = None
        tree_horzt_sub['pct_submassa_isin_cnpb'] = 1.0
        tree_horzt_sub['CODCART'] = None

        max_depth = tree_horzt_sub['nivel'].max()

        if pd.isna(max_depth):
            return tree_horzt_sub

        for i in range(0, max_depth + 1):
            suffix = '' if i == 0 else f'_nivel_{i}'
            isin_col = f"isin{suffix}"

            tree_horzt_sub = tree_horzt_sub.merge(
                port_submassa[mask_port][cols_port_submassa],
                left_on=['dtposicao', 'cnpb', isin_col],
                right_on=['dtposicao', 'CNPB', 'isin'],
                how='left',
                suffixes=('', f"_{suffix}"),
                indicator=True,
            )

            mask_merge = (tree_horzt_sub['_merge'] == 'both')

            tree_horzt_sub.loc[mask_merge, 'COD_SUBMASSA'] = tree_horzt_sub[f"COD_SUBMASSA_{suffix}"]
            tree_horzt_sub.loc[mask_merge, 'SUBMASSA'] = tree_horzt_sub[f"SUBMASSA_{suffix}"]
            tree_horzt_sub.loc[mask_merge, 'pct_submassa_isin_cnpb'] = tree_horzt_sub[f"pct_submassa_isin_cnpb_{suffix}"]
            tree_horzt_sub.loc[mask_merge, 'CODCART'] = tree_horzt_sub[f"CODCART_{suffix}"]

            tree_horzt_sub.drop(columns=['_merge'], inplace=True)

    tree_horzt_sub['pct_submassa_isin_cnpb'] = tree_horzt_sub['pct_submassa_isin_cnpb'].astype(float).fillna(1.0)
    mask_bsps = (tree_horzt_sub['SUBMASSA'].isna())
    tree_horzt_sub.loc[mask_bsps, 'COD_SUBMASSA'] = '1'
    tree_horzt_sub.loc[mask_bsps, 'SUBMASSA'] = 'BSPS'

    debug_save(tree_horzt_sub, 'submassa-arvore-pct_part', debug_cfg, 'tree', 'debug_save_tree_submassa_pct_part')

    return tree_horzt_sub


def enrich_horizontal_tree(tree_horzt, governance_struct):
    with log_timing('tree', 'enrich_values'):
        enrich_values(tree_horzt)

    with log_timing('tree', 'enrich_text'):
        enrich_text(tree_horzt)

    with log_timing('tree', 'governance_struct'):
        governance_struct = governance_struct[governance_struct['KEY_VEICULO'].notna()]

        assign_governance_struct_keys(tree_horzt, governance_struct)


def load_config():
    config = utl.load_config('config.ini')

    if not config.has_section('Debug'):
        raise KeyError('Missing [Debug] section in config.ini')

    if not config.has_section('Paths'):
        raise KeyError('Missing [Paths] section in config.ini')

    xml_source_path = config['Paths']['xml_source_path']
    xml_source_path = f"{os.path.dirname(utl.format_path(xml_source_path))}/"

    destination_path = config['Paths']['destination_path']
    destination_path = f"{os.path.dirname(utl.format_path(destination_path))}/"

    destination_file_format = config['Paths']['destination_file_format']

    data_aux_path = config['Paths']['data_aux_path']
    data_aux_path = f"{os.path.dirname(utl.format_path(data_aux_path))}/"

    mec_sac_path = config['Paths']['mec_sac_path']
    mec_sac_path = f"{os.path.dirname(utl.format_path(mec_sac_path))}/"

    debug_cfg = {
        'save': config['Debug'].get('debug').lower() == 'yes',
        'output_path': config['Debug'].get('debug_path'),
        'file_format': config['Debug'].get('debug_file_format')
    }

    evidence_root = config['Paths']['log_evidence_root']
    evidence_root = f"{os.path.dirname(utl.format_path(evidence_root))}/"
    log_cfg = {
        'log_evidence_root': evidence_root,
        'log_evidence_file_format': config['Paths']['log_evidence_file_format'],
    }

    return [xml_source_path, destination_path, destination_file_format,
            data_aux_path, debug_cfg, log_cfg, mec_sac_path]


def compute_plan_returns_adjust(debug_cfg, tree_hrztl, dcadplanosac,
                                mec_sac_path, processes, port_submassa):
    mec_sac = load_mecsac(mec_sac_path, processes)

    with log_timing('plans_returns', 'compute_adjustment'):
        mec_sac_returns_by_plan, tree_returns_by_plan, plan_returns_adjust = (
            compute_plan_returns_adjustment(tree_hrztl, mec_sac, dcadplanosac, port_submassa)
            )

    debug_save(mec_sac_returns_by_plan, 'rentab-plano-mecsac', debug_cfg, 'tree', 'debug_save_compute_returns_adjust')
    debug_save(tree_returns_by_plan, 'rentab-plano-tree', debug_cfg, 'tree', 'debug_save_compute_returns_adjust')
    debug_save(plan_returns_adjust , 'rentab-plano-ajuste', debug_cfg, 'tree', 'debug_save_compute_returns_adjust')

    with log_timing('plans_returns', 'enrich_adjustment'):

        cols_adjust = ['cnpb', 'dtposicao', 'contribution_ajuste_rentab',
                   'contribution_ajuste_rentab_fator', 'CODCART']

        adjust_rentab = plan_returns_adjust[cols_adjust].merge(
            dcadplanosac[['CODCART', 'COD_SUBMASSA', 'SUBMASSA']],
            on=['CODCART'],
            how='left',
            )
        adjust_rentab.rename(columns={'contribution_ajuste_rentab': 'contribution_rentab_ponderada'}, inplace=True)
        adjust_rentab['nivel'] = 0
        cols_adjust = ['KEY_ESTRUTURA_GERENCIAL', 'codcart', 'nome', 'NEW_TIPO',
                    'NEW_NOME_ATIVO', 'SEARCH', 'NEW_TIPO_FINAL',
                    'NEW_NOME_ATIVO_FINAL', 'isin', 'contribution_ativo', 'contribution_match']
        for col in cols_adjust:
            adjust_rentab[col] = '#AJUSTE'

        cols_adjust = ['fEMISSOR.NOME_EMISSOR', 'NEW_GESTOR', 'NEW_GESTOR_WORD_CLOUD',
                    'NEW_NOME_ATIVO_FINAL', 'NEW_GESTOR_WORD_CLOUD_FINAL',
                    'fEMISSOR.NOME_EMISSOR_FINAL']
        for col in cols_adjust:
            adjust_rentab[col] = 'VIVEST'

        adjust_rentab['CODCART'] = adjust_rentab['CODCART'].fillna('')

        return adjust_rentab


def assign_adjustments(tree_hrztl, adjust_rentab):
    with log_timing('plans_returns', 'assign_adjustment'):
        tree_hrztl = tree_hrztl.merge(
            adjust_rentab[['cnpb', 'CODCART', 'dtposicao', 'contribution_ajuste_rentab_fator']],
            on=['cnpb', 'CODCART', 'dtposicao'],
            how='left',
            )
        tree_hrztl['contribution_rentab_ponderada_ajustada'] = (
            tree_hrztl['contribution_rentab_ponderada']
            * tree_hrztl['contribution_ajuste_rentab_fator']
            )

        tree_hrztl = pd.concat([tree_hrztl, adjust_rentab])

    return tree_hrztl


def run_pipeline():
    locale.setlocale(locale.LC_ALL, '')

    (
        xml_source_path,
        destination_path,
        destination_file_format,
        data_aux_path,
        debug_cfg,
        log_cfg,
        mec_sac_path,
    ) = load_config()

    setup_folders([destination_path])

    with log_timing('load', 'load_dbaux'):
        db_aux = aux_loader.load_dbaux(data_aux_path)

    funds_dtypes = dta.read('fundos_metadata')
    port_dtypes = dta.read('carteiras_metadata')

    header_daily_values = dta.read('header_daily_values')
    daily_keys = header_daily_values.keys()
    types_series = [key for key, value in header_daily_values.items() if value.get('serie', False)]

    processes = min(8, multiprocessing.cpu_count())
    numeric_fields = create_numeric_fields_set(funds_dtypes, port_dtypes)
    funds, portfolios = parse_files(debug_cfg, xml_source_path,
                                    processes, daily_keys, numeric_fields)

    types_to_exclude = dta.read('types_to_exclude')
    harmonization_rules = dta.read('harmonization_values_rules')

    funds, portfolios = clean_and_prepare_raw(debug_cfg, funds, portfolios,
                                              types_to_exclude, types_series,
                                              harmonization_rules, funds_dtypes, port_dtypes)

    name_standardization_rules = dta.read('name_standardization_rules')
    new_tipo_rules = dta.read('enrich_de_para_tipos')
    gestor_name_stopwords = dta.read('gestor_name_stopwords')

    portfolios = explode_partplanprev(debug_cfg, portfolios)

    funds, portfolios = enrich(debug_cfg, funds, portfolios, types_series, data_aux_path,
                               db_aux['dcadplano'], new_tipo_rules, gestor_name_stopwords,
                               name_standardization_rules)

    check_values_integrity(log_cfg, funds, 'fundos', funds, ['cnpj'])
    check_values_integrity(log_cfg, portfolios, 'carteiras', funds, ['cnpjcpf', 'codcart'])

    check_puposicao_consistency(log_cfg, funds, portfolios)

    validate_fund_graph_is_acyclic(funds)

    compute_metrics(funds, portfolios, types_series)

    assign_returns(funds, ['cnpj'], 'fundos')
    assign_returns(portfolios, ['cnpjcpf', 'codcart', 'cnpb'], 'carteiras')

    portfolios, port_submassa = extract_portfolio_submassa(debug_cfg, db_aux['dcadsubmassa'], portfolios)
    compute_composition_portfolio_submassa(debug_cfg, port_submassa)

    tree_hrztl, tree_hrztl_sub = build_horizontal_tree(debug_cfg, funds, portfolios, port_submassa)
    tree_hrztl_sub = explode_horizontal_tree_submassa(debug_cfg, tree_hrztl_sub, port_submassa)
    tree_hrztl = pd.concat([tree_hrztl, tree_hrztl_sub], ignore_index=True)
    #Preenche CODCART com vazio para as demais partes do codigo que passam a usar essa coluna
    #para agregacoes
    tree_hrztl['CODCART'] = tree_hrztl['CODCART'].fillna('')
    enrich_horizontal_tree(tree_hrztl, db_aux['governance_struct'])

    adjust_rentab = compute_plan_returns_adjust(debug_cfg, tree_hrztl,
                                                db_aux['dcadplanosac'], mec_sac_path,
                                                processes, port_submassa)

    tree_hrztl = assign_adjustments(tree_hrztl, adjust_rentab)

    with log_timing('finish', 'save_final_files'):
        save_df(portfolios, f"{destination_path}carteiras", destination_file_format)
        save_df(funds,      f"{destination_path}fundos",    destination_file_format)
        save_df(tree_hrztl, f"{destination_path}arvore_carteiras", destination_file_format)


if __name__ == "__main__":
    start_time = datetime.now()
    with log_timing('full', 'all_process'):
        run_pipeline()
    print(f"Execucao: {start_time} -> {datetime.now()}")
