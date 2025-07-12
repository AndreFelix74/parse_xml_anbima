#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  8 16:51:15 2025

@author: andrefelix
"""


import os
import locale
from logger import log_timing

import auxiliary_loaders as aux_loader
import data_access as dta
import util as utl
from file_handler import save_df

import pandas as pd


def prepare_paths():
    """
    Load and format all relevant directory paths from the config.ini file.

    Returns:
        dict: A dictionary containing cleaned paths for xlsx output, auxiliary data, mec_sac data, and performance data.
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
        struct = aux_loader.load_performance_struct(paths['aux'])
        struct['PERFIL_BASE'] = struct['PERFIL_BASE'].astype(str).str.strip().str.upper()

    with log_timing('performance', 'load_mec_sac'):
        mec_sac = aux_loader.load_mec_sac_last_day_month(paths['mec_sac'])
        mec_sac['MES_ANO'] = mec_sac['DT'].dt.to_period('M').dt.to_timestamp()

    return plano_de_para, dcadplanosac, struct, mec_sac


def process_performance(performance, struct):
    """
    Clean and merge the performance dataset with struct definitions.

    Args:
        performance (DataFrame): Raw performance data.
        struct (DataFrame): Struct data for merging.

    Returns:
        DataFrame: Filtered and enriched performance dataset.
    """
    performance['TIPO_PLANO'] = performance['PLANO'].str.split('-').str[1].fillna('').str.strip()
    performance.loc[performance['PLANO'] == 'ROCHOPREV', 'TIPO_PLANO'] = 'CV'

    mask_cd = performance['TIPO_PLANO'].isin(['', 'AGRESSIVO', 'MODERADO', 'CONSERVADOR'])
    performance.loc[mask_cd, 'TIPO_PLANO'] = 'CD'

    performance = performance.merge(struct, how='left', on='PERFIL_BASE', suffixes=('', '_estr'))
    
    meses_pt = {
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
        'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
        'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
    }
    
    performance[['mes_nome', 'ano_str']] = performance['DATA'].str.lower().str.strip().str.split('-', expand=True)
    
    # Converte mês
    performance['mes'] = performance['mes_nome'].map(meses_pt)
    
    # Trata o ano (converte para dois dígitos inteiros, e depois para quatro dígitos)
    performance['ano'] = performance['ano_str'].str[-2:].astype(int) + 2000
    performance.loc[performance['ano'] < 100, 'ano'] += 2000  # assume anos < 100 como dois dígitos
    
    # Converte para datetime (primeiro dia do mês)
    performance['DATA'] = pd.to_datetime(
        dict(year=performance['ano'], month=performance['mes'], day=1),
        errors='coerce'
    )

    return performance[performance['TIPO_PERFIL_BASE'] != 'A']


def calc_mec_sac_returns(mec_sac, dcadplanosac):
    """
    Calculate weighted monthly returns for each plan from mec_sac and dcadplanosac.

    Args:
        mec_sac (DataFrame): MEC/SAC return data.
        dcadplanosac (DataFrame): Client-plan mapping.

    Returns:
        DataFrame: Weighted monthly returns by plan and period.
    """
    df = mec_sac.merge(dcadplanosac, how='left', left_on='CODCLI', right_on='CODCLI_SAC')
    df['total_pl'] = df.groupby(['NOME_PLANO_KEY_DESEMPENHO', 'MES_ANO'])['VL_PATRLIQTOT1'].transform('sum')
    df['RENTAB_MES_PONDERADA'] = (df['VL_PATRLIQTOT1'] / df['total_pl']) * df['RENTAB_MES']
    return df.groupby(['NOME_PLANO_KEY_DESEMPENHO', 'MES_ANO'], as_index=False)['RENTAB_MES_PONDERADA'].sum()


def calc_performance_returns(performance):
    """
    Calculate weighted monthly returns for each plan from the performance dataset.

    Args:
        performance (DataFrame): Cleaned performance data.

    Returns:
        DataFrame: Aggregated performance return by plan and date.
    """
    performance['total_pl'] = performance.groupby(['PLANO', 'DATA'])['PL'].transform('sum')
    performance['RENTAB_MES_PONDERADA_DESEMPENHO'] = (performance['PL'] / performance['total_pl']) * performance['RETORNO_MES']
    return performance.groupby(['PLANO', 'DATA'], as_index=False)['RENTAB_MES_PONDERADA_DESEMPENHO'].sum()


def merge_and_adjust_returns(perf_grouped, mec_sac_returns, plano_de_para):
    """
    Merge performance and MEC/SAC returns and compute adjustment differences.

    Args:
        perf_grouped (DataFrame): Aggregated performance returns.
        mec_sac_returns (DataFrame): Aggregated MEC/SAC returns.
        plano_de_para (dict): Renaming dictionary for plan names.

    Returns:
        DataFrame: Final merged and adjusted dataset with rentability difference.
    """
    perf_grouped['NEW_PLANO'] = perf_grouped['PLANO'].map(plano_de_para).fillna(perf_grouped['PLANO'])
    merged = perf_grouped.merge(
        mec_sac_returns,
        left_on=['NEW_PLANO', 'DATA'],
        right_on=['NOME_PLANO_KEY_DESEMPENHO', 'MES_ANO'],
        how='left'
    )
    merged['ajuste_rentab'] = merged['RENTAB_MES_PONDERADA_DESEMPENHO'] - merged['RENTAB_MES_PONDERADA']
    return merged


def run_pipeline():
    """
    Main execution pipeline that processes auxiliary files and performance data,
    calculates weighted returns, compares performance vs. MEC/SAC returns,
    and saves the final adjustment file.
    """
    locale.setlocale(locale.LC_ALL, '')
    paths = prepare_paths()

    plano_de_para, dcadplanosac, struct, mec_sac = load_auxiliary_data(paths)

    with log_timing('performance', 'load_performance'):
        performance = aux_loader.load_performance(paths['performance'])

    performance = process_performance(performance, struct)
    mec_sac_returns = calc_mec_sac_returns(mec_sac, dcadplanosac)
    perf_grouped = calc_performance_returns(performance)

    perf_adjusted = merge_and_adjust_returns(perf_grouped, mec_sac_returns, plano_de_para)
    save_df(perf_adjusted, f"{paths['xlsx']}ajuste_desempenho", 'csv')


if __name__ == "__main__":
    with log_timing('full', 'all_process'):
        run_pipeline()
