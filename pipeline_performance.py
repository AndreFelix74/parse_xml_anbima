#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  8 16:51:15 2025

@author: andrefelix
"""


import os
import locale
import pandas as pd
from logger import log_timing

import auxiliary_loaders as aux_loader
import data_access as dta
import util as utl
from file_handler import save_df



def prepare_paths():
    """
    Load and format all relevant directory paths from the config.ini file.

    Returns:
        dict: A dictionary containing cleaned paths for xlsx output,
            auxiliary data, mec_sac data, and performance data.
    """
    config = utl.load_config('config.ini')
    return {
        'xlsx': f"{os.path.dirname(utl.format_path(config['Paths']['xlsx_destination_path']))}/",
        'aux': f"{os.path.dirname(utl.format_path(config['Paths']['data_aux_path']))}/",
        'mec_sac': f"{os.path.dirname(utl.format_path(config['Paths']['mec_sac_path']))}/",
        'performance': f"{os.path.dirname(utl.format_path(config['Paths']['performance_path']))}/"
    }


def load_auxiliary_data(paths):
    """
    Load all auxiliary datasets: renaming mappings, struct definitions, and mec_sac returns.

    Args:
        paths (dict): Dictionary containing directory paths.

    Returns:
        tuple: (plan renaming dict, dcadplanosac DataFrame, struct DataFrame, mec_sac DataFrame)
    """
    plano_de_para = dta.read('planos_desempenho_renaming')

    with log_timing('plans_returns', 'load_dcadplanosac'):
        dcadplanosac = aux_loader.load_dcadplanosac(paths['aux'])

    with log_timing('performance', 'load_struct'):
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

    performance['TIPO_PLANO'] = performance['PLANO'].str.split('-').str[1].fillna('').str.strip()

    performance['PLANO'] = performance['PLANO'].map(
        plano_de_para).fillna(performance['PLANO'])

    performance.loc[performance['PLANO'] == 'ROCHEPREV', 'TIPO_PLANO'] = 'CV'
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
    mec_sac_dcadplanosac['total_pl'] = mec_sac_dcadplanosac.groupby(
        ['NOME_PLANO_KEY_DESEMPENHO', 'DT'])['VL_PATRLIQTOT1'].transform('sum')

    mec_sac_dcadplanosac['RENTAB_MES_PONDERADA'] = (
        (mec_sac_dcadplanosac['VL_PATRLIQTOT1']
         / mec_sac_dcadplanosac['total_pl'])
        * mec_sac_dcadplanosac['RENTAB_MES']
    )

    return mec_sac_dcadplanosac.groupby(
        ['NOME_PLANO_KEY_DESEMPENHO', 'DT'], as_index=False)['RENTAB_MES_PONDERADA'].sum()


def calc_performance_returns(performance):
    """
    Calculate weighted monthly returns for each plan from the performance dataset.

    Args:
        performance (DataFrame): Cleaned performance data.

    Returns:
        DataFrame: Aggregated performance return by plan and date.
    """
    weighted_returns = performance.copy()
    cols_group = ['PLANO', 'DATA', 'TIPO_PLANO']
    weighted_returns['total_pl'] = weighted_returns.groupby(cols_group)['PL'].transform('sum')
    weighted_returns['RENTAB_MES_PONDERADA_DESEMPENHO'] = (
        (weighted_returns['PL']
         / weighted_returns['total_pl'])
        * weighted_returns['RETORNO_MES']
        )

    return weighted_returns.groupby(
        ['PLANO', 'DATA', 'TIPO_PLANO'], as_index=False)['RENTAB_MES_PONDERADA_DESEMPENHO'].sum()


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
    parsed_date = pd.Series(pd.NaT, index=performance.index)

    coerced_datetime = pd.to_datetime(performance['DATA'], format='%d/%m/%Y', errors='coerce')

    parsed_date[coerced_datetime.notna()] = (
        coerced_datetime[coerced_datetime.notna()]
        .dt.to_period('M')
        .dt.to_timestamp()
    )

    series_str = performance['DATA'].astype(str).str.strip().str.lower()

    regex_aa = r'^([a-zçã]+)-(\d{2})$'
    regex_aaaa = r'^([a-zçã]+)-(\d{4})$'

    extract_aa = series_str.str.extract(regex_aa)
    extract_aaaa = series_str.str.extract(regex_aaaa)

    mask = extract_aa.notna().all(axis=1)
    if mask.any():
        mes = extract_aa.loc[mask, 0].map(months_pt).astype('Int64')
        ano = extract_aa.loc[mask, 1].astype(int) + 2000
        parsed_date[mask] = pd.to_datetime({'year': ano, 'month': mes, 'day': 1})

    mask = extract_aaaa.notna().all(axis=1)
    if mask.any():
        mes = extract_aaaa.loc[mask, 0].map(months_pt).astype('Int64')
        ano = extract_aaaa.loc[mask, 1].astype(int)
        parsed_date[mask] = pd.to_datetime({'year': ano, 'month': mes, 'day': 1})

    performance['DATA'] = parsed_date

    erros = parsed_date.isna()
    if erros.any():
        print(f"[parse_data_mes_ano_pt] Erros em {erros.sum()} linhas:")
        print(performance.loc[erros, 'DATA'])


def calc_adjust(perf_returns_by_plan, mec_sac_returns):
    """
    Calculates the monthly return adjustment by comparing two sources of performance data.

    This function merges performance returns by plan with another dataset containing
    adjusted returns (e.g., from a different calculation method or data source),
    computes the monthly difference between them, and returns a simplified DataFrame
    with relevant columns.

    Args:
        perf_returns_by_plan (pd.DataFrame): A DataFrame containing monthly performance returns
            per plan, with columns like 'PLANO' and 'DATA'.
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
    """
    merged = perf_returns_by_plan.merge(
        mec_sac_returns,
        left_on=['PLANO', 'DATA'],
        right_on=['NOME_PLANO_KEY_DESEMPENHO', 'DT'],
        how='left'
    )

    merged['ajuste_rentab'] = (
        merged['RENTAB_MES_PONDERADA_DESEMPENHO']
        - merged['RENTAB_MES_PONDERADA']
        )

    merged.rename(columns={'ajuste_rentab': 'RETORNO_MES'}, inplace=True)
    merged['PERFIL_BASE'] = '#AJUSTE'
    cols_adjust = ['PERFIL_BASE','PLANO', 'DATA', 'TIPO_PLANO',
                   'NOME_PLANO_KEY_DESEMPENHO', 'RETORNO_MES']
    return merged[cols_adjust]


def run_pipeline():
    """
    Main execution pipeline that processes auxiliary files and performance data,
    calculates weighted returns, compares performance vs. MEC/SAC returns,
    and saves the final adjustment file.
    """
    locale.setlocale(locale.LC_ALL, '')
    paths = prepare_paths()

    plano_de_para, dcadplanosac, struct_perform = load_auxiliary_data(paths)
    struct_perform['PERFIL_BASE'] = (
        struct_perform['PERFIL_BASE']
        .astype(str)
        .str.strip()
        .str.upper()
    )

    with log_timing('performance', 'load_mec_sac'):
        mec_sac = aux_loader.load_mec_sac_last_day_month(paths['mec_sac'])

    mec_sac['DT'] = mec_sac['DT'].dt.to_period('M').dt.to_timestamp()

    with log_timing('performance', 'load_performance'):
        performance = aux_loader.load_performance(paths['performance'])

    standardize_performance_plans(performance, plano_de_para)
    parse_date_pt(performance)

    mec_sac_dcadplanosac = mec_sac.merge(
        dcadplanosac,
        how='left',
        left_on='CODCLI',
        right_on='CODCLI_SAC'
        )
    mec_sac_returns = calc_mec_sac_returns(mec_sac_dcadplanosac)

    perf_returns_by_plan = calc_performance_returns(performance)

    performance_adjust = calc_adjust(perf_returns_by_plan, mec_sac_returns)
    result = pd.concat([performance, performance_adjust])

    result = result.merge(struct_perform, how='left', on='PERFIL_BASE', suffixes=('', '_estr'))
    result = result[result['TIPO_PERFIL_BASE'] != 'A']
    save_df(result, f"{paths['xlsx']}ajuste_desempenho", 'csv')


if __name__ == "__main__":
    with log_timing('full', 'all_process'):
        run_pipeline()
