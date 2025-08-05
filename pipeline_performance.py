#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  8 16:51:15 2025

@author: andrefelix
"""


import os
import locale
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
from logger import log_timing, RUN_ID

import auxiliary_loaders as aux_loader
import data_access as dta
import util as utl
from file_handler import save_df


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


def prepare_paths():
    """
    Load and format all relevant directory paths from the config.ini file.

    Returns:
        tuple: (paths dict, intermediate_cfg dict)
    """
    config = utl.load_config('config.ini')

    # Verificações de seções obrigatórias
    if not config.has_section('Paths'):
        raise KeyError('Missing [Paths] section in config.ini')

    if not config.has_section('Debug'):
        raise KeyError('Missing [Debug] section in config.ini')

    paths = {
        'xlsx': f"{os.path.dirname(utl.format_path(config['Paths']['xlsx_destination_path']))}/",
        'aux': f"{os.path.dirname(utl.format_path(config['Paths']['data_aux_path']))}/",
        'mec_sac': f"{os.path.dirname(utl.format_path(config['Paths']['mec_sac_path']))}/",
        'performance': f"{os.path.dirname(utl.format_path(config['Paths']['performance_path']))}/"
    }

    intermediate_cfg = {
        'save': config['Debug'].get('write_intermediate_files', '').lower() == 'yes',
        'output_path': config['Debug'].get('intermediate_output_path'),
        'file_format': config['Paths'].get('destination_file_extension')
    }

    return paths, intermediate_cfg


def find_all_performance_files(files_path):
    """
    Recursively finds all files Desempenho, and returns metadata.

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
        if file.lower().startswith('desempenho')
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


def load_performance(intermediate_cfg, performance_source_path, plano_de_para, processes):
    with log_timing('load', 'find_performance_files') as log:
        all_performance_files = find_all_performance_files(performance_source_path)

        log.info(
            'load',
            total=len(all_performance_files),
        )

    with log_timing('load', 'load_performance_content') as log:
        with ProcessPoolExecutor(max_workers=processes) as executor:
            dfs = list(executor.map(aux_loader.load_performance, all_performance_files))

    performance = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if intermediate_cfg['save']:
        with log_timing('load', 'save_performance_raw_data') as log:
            save_intermediate(performance, 'desempenho-raw', intermediate_cfg, log)

    mask = ~performance['PLANO'].str.contains('TOTAL', case=False, na=False)
    performance = performance[mask]
    performance['PL'] *= 1_000 # as planilhas de desempenho estao em milhares
    standardize_performance_plans(performance, plano_de_para)
    parse_date_pt(performance)

    if intermediate_cfg['save']:
        with log_timing('load', 'save_performance_parsed_data') as log:
            save_intermediate(performance, 'desempenho-parsed', intermediate_cfg, log)

    return performance


def load_mecsac(intermediate_cfg, mec_source_path, processes):
    with log_timing('load', 'find_mecsac_files') as log:
        all_mecsac_files = find_all_mecsac_files(mec_source_path)

        log.info(
            'load',
            total=len(all_mecsac_files),
        )

    with log_timing('load', 'load_mecsac_content') as log:
        with ProcessPoolExecutor(max_workers=processes) as executor:
            dfs = list(executor.map(aux_loader.load_mecsac_last_day_month_by_file,
                                    all_mecsac_files))

    mec_sac = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if intermediate_cfg['save']:
        with log_timing('load', 'save_mec_raw_data') as log:
            save_intermediate(mec_sac, 'mec_sac-raw', intermediate_cfg, log)

    mec_sac['DT'] = mec_sac['DT'].dt.to_period('M').dt.to_timestamp()

    if intermediate_cfg['save']:
        with log_timing('load', 'save_mec_parsed_data') as log:
            save_intermediate(mec_sac, 'mec_sac-parsed', intermediate_cfg, log)

    return mec_sac


def load_auxiliary_data(paths):
    """
    Load all auxiliary datasets: renaming mappings, struct definitions, and mec_sac returns.

    Args:
        paths (dict): Dictionary containing directory paths.

    Returns:
        tuple: (plan renaming dict, dcadplanosac DataFrame, struct DataFrame, mec_sac DataFrame)
    """
    plano_de_para = dta.read('planos_desempenho_renaming')

    with log_timing('load', 'dcadplanosac'):
        dcadplanosac = aux_loader.load_dcadplanosac(paths['aux'], 'contencioso')

    with log_timing('load', 'struct'):
        struct_perform = aux_loader.load_performance_struct(paths['aux'])

    return plano_de_para, dcadplanosac, struct_perform


def standardize_performance_plans(performance, plano_de_para):
    """
    Clean the performance dataset.

    Args:
        performance (DataFrame): Raw performance data.

    Returns:
        None: Changes in place.
    """
    performance['PLANO'] = performance['PLANO'].str.upper().str.strip()

    performance['PLANO'] = performance['PLANO'].map(
        plano_de_para).fillna(performance['PLANO'])

    performance['TIPO_PLANO'] = performance['PLANO'].str.split('-').str[1].fillna('').str.strip()

    mask_cd = performance['TIPO_PLANO'].isin(['', 'AGRESSIVO', 'MODERADO', 'CONSERVADOR'])
    performance.loc[mask_cd, 'TIPO_PLANO'] = 'CD'

    performance['PLANO'] = performance['PLANO'].str.replace('-', ' ', regex=False)
    performance['PLANO'] = performance['PLANO'].str.replace(r'\s+', ' ', regex=True).str.strip()
    performance['PLANO'] = performance['PLANO'].str.strip()


def calc_mec_sac_returns(mec_sac_dcadplanosac):
    """
    Calculate weighted monthly returns for each plan from mec_sac and dcadplanosac.

    Args:
        mec_sac (DataFrame): MEC/SAC return data.
        dcadplanosac (DataFrame): Client-plan mapping.

    Returns:
        DataFrame: Weighted monthly returns by plan and period.
    """
    mec_sac_dcadplanosac['TOTAL_PL_MEC_SAC'] = mec_sac_dcadplanosac.groupby(
        ['NOME_PLANO_KEY_DESEMPENHO', 'DT'])['VL_PATRLIQTOT1'].transform('sum')

    mec_sac_dcadplanosac['RENTAB_MES_PONDERADA_MEC_SAC'] = (
        (mec_sac_dcadplanosac['VL_PATRLIQTOT1']
         / mec_sac_dcadplanosac['TOTAL_PL_MEC_SAC'])
        * mec_sac_dcadplanosac['RENTAB_MES']
    )

    cols_group = ['NOME_PLANO_KEY_DESEMPENHO', 'DT', 'TOTAL_PL_MEC_SAC']
    return mec_sac_dcadplanosac.groupby(cols_group,
                                        as_index=False)['RENTAB_MES_PONDERADA_MEC_SAC'].sum()


def calc_performance_returns(performance):
    """
    Calculate weighted monthly returns for each plan from the performance dataset.

    Args:
        performance (DataFrame): Cleaned performance data.

    Returns:
        DataFrame: Aggregated performance return by plan and date.
    """
    weighted_returns = performance[performance['PERFIL_N2'] != 'Previdenciário'].copy()
    cols_group = ['PLANO', 'DATA', 'TIPO_PLANO']
    weighted_returns['TOTAL_PL_DESEMPENHO'] = (
        weighted_returns.groupby(cols_group)['PL'].transform('sum')
        )
    weighted_returns['RETORNO_MES_PONDERADO_DESEMPENHO'] = (
        (weighted_returns['PL']
         / weighted_returns['TOTAL_PL_DESEMPENHO'])
        * weighted_returns['RETORNO_MES']
        )

    cols_group = ['PLANO', 'DATA', 'TIPO_PLANO', 'TOTAL_PL_DESEMPENHO']
    return weighted_returns.groupby(cols_group,
                                    as_index=False)['RETORNO_MES_PONDERADO_DESEMPENHO'].sum()


def parse_date_pt(performance):
    """
    Converte coluna com datas em datetime ou 'mes-ano' (pt-br) para datetime
        no formato primeiro dia do mês.
    """
    months_pt = {
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
        'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
        'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
    }

    coerced_datetime = pd.to_datetime(performance['DATA'], errors='coerce', dayfirst=True)
    mask_dt = coerced_datetime.notna()

    parsed_date = pd.Series(pd.NaT, index=performance.index)
    parsed_date[mask_dt] = (
        coerced_datetime[mask_dt]
        .dt.to_period('M')
        .dt.to_timestamp()
    )

    mask_remaining = ~mask_dt
    series_str = (
        performance.loc[mask_remaining, 'DATA']
        .astype(str)
        .str.strip()
        .str.lower()
    )

    regex_aa = r'^([a-zçã]+)-(\d{2})$'
    regex_aaaa = r'^([a-zçã]+)-(\d{4})$'

    extract_aa = series_str.str.extract(regex_aa)
    extract_aaaa = series_str.str.extract(regex_aaaa)

    mask = extract_aa.notna().all(axis=1)
    if mask.any():
        mes = extract_aa.loc[mask, 0].map(months_pt).astype('Int64')
        ano = extract_aa.loc[mask, 1].astype(int) + 2000
        parsed_date.loc[mask.index] = pd.to_datetime(
            {'year': ano, 'month': mes, 'day': 1}
        )

    mask = extract_aaaa.notna().all(axis=1)
    if mask.any():
        mes = extract_aaaa.loc[mask, 0].map(months_pt).astype('Int64')
        ano = extract_aaaa.loc[mask, 1].astype(int)
        parsed_date.loc[mask.index] = pd.to_datetime(
            {'year': ano, 'month': mes, 'day': 1}
        )

    performance['DATA'] = parsed_date

    erros = parsed_date.isna()
    if erros.any():
        print(f"[parse_date_pt] Erros em {erros.sum()} linhas:")
        print(performance.loc[erros, 'DATA'])


def calc_adjust(perf_returns_by_plan, mec_sac_returns):
    """
    Calculates the monthly return adjustment by comparing two sources of performance data.

    Args:
        perf_returns_by_plan (pd.DataFrame): A DataFrame containing monthly performance
            returns per plan, with columns like 'PLANO' and 'DATA'.
        mec_sac_returns (pd.DataFrame): A DataFrame with reference or adjusted returns,
            containing columns like 'NOME_PLANO_KEY_DESEMPENHO' and 'DT'.

    Returns:
        pd.DataFrame: A DataFrame with the columns:
            - 'PERFIL_BASE'
            - 'PLANO'
            - 'DATA'
            - 'PLANO'
            - 'NOME_PLANO_KEY_DESEMPENHO'
            - 'RETORNO_MES': the difference between the reference and original monthly return.
            - 'PL': the difference between the reference and original monthly return.
    """
    adjust_returns = perf_returns_by_plan.merge(
        mec_sac_returns,
        left_on=['PLANO', 'DATA'],
        right_on=['NOME_PLANO_KEY_DESEMPENHO', 'DT'],
        how='left'
    )

    adjust_returns['ajuste_rentab'] = (
        adjust_returns['RENTAB_MES_PONDERADA_MEC_SAC']
        - adjust_returns['RETORNO_MES_PONDERADO_DESEMPENHO']
        )

    adjust_returns['ajuste_pl'] = (
        adjust_returns['TOTAL_PL_MEC_SAC']
        - adjust_returns['TOTAL_PL_DESEMPENHO']
        )

    adjust_returns.rename(columns={'ajuste_rentab': 'RETORNO_MES',
                                   'ajuste_pl': 'PL',}, inplace=True)
    adjust_returns['PERFIL_BASE'] = '#AJUSTE'

    cols_adjust = ['PERFIL_BASE','PLANO', 'DATA', 'TIPO_PLANO',
                   'NOME_PLANO_KEY_DESEMPENHO', 'RETORNO_MES', 'PL', 'TOTAL_PL_MEC_SAC']

    return adjust_returns[cols_adjust]


def merge_and_filter_struct(dfrm, struct_perform):
    """
    Performs a left merge between the input DataFrame and the struct_perform DataFrame
    using the 'PERFIL_BASE' column, then filters out rows where 'TIPO_PERFIL_BASE' is 'A'.

    Args:
        df (pd.DataFrame): The input DataFrame to be merged and filtered.
        struct_perform (pd.DataFrame): DataFrame containing structural profile information,
                                       including 'PERFIL_BASE' and 'TIPO_PERFIL_BASE' columns.

    Returns:
        pd.DataFrame: The resulting DataFrame after merging and filtering.
    """
    dfrm = dfrm.merge(struct_perform, how='left', on='PERFIL_BASE')
    return dfrm[dfrm['TIPO_PERFIL_BASE'] != 'A']


def run_pipeline():
    """
    Main execution pipeline that processes auxiliary files and performance data,
    calculates weighted returns, compares performance vs. MEC/SAC returns,
    and saves the final adjustment file.
    """
    locale.setlocale(locale.LC_ALL, '')
    paths, intermediate_cfg = prepare_paths()

    plano_de_para, dcadplanosac, struct_perform = load_auxiliary_data(paths)
    struct_perform['PERFIL_BASE'] = (
        struct_perform['PERFIL_BASE']
        .astype(str)
        .str.strip()
        .str.upper()
    )

    processes = min(8, multiprocessing.cpu_count())

    mec_sac = load_mecsac(intermediate_cfg, paths['mec_sac'], processes)

    performance = load_performance(intermediate_cfg, paths['performance'],
                                   plano_de_para, processes)

    with log_timing('performance', 'computations') as log:
        performance = merge_and_filter_struct(performance, struct_perform)

        perf_returns_by_plan = calc_performance_returns(performance)

        mec_sac_dcadplanosac = mec_sac.merge(
            dcadplanosac,
            how='left',
            left_on='CODCLI',
            right_on='CODCLI_SAC'
            )
        mec_sac_returns = calc_mec_sac_returns(mec_sac_dcadplanosac)

        performance_adjust = calc_adjust(perf_returns_by_plan, mec_sac_returns)
        performance_adjust = merge_and_filter_struct(performance_adjust,
                                                     struct_perform)

        result = pd.concat([performance, performance_adjust])

    if intermediate_cfg['save']:
        with log_timing('performance', 'save_intermediate_files') as log:
            save_intermediate(perf_returns_by_plan, 'perf_returns_by_plan-raw',
                              intermediate_cfg, log)
            save_intermediate(mec_sac_returns, 'mec_sac_returns-raw',
                              intermediate_cfg, log)

    save_df(result, f"{paths['xlsx']}desempenho", 'csv')


if __name__ == "__main__":
    with log_timing('full', 'all_process'):
        run_pipeline()
